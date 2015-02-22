{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE OverloadedStrings #-}

module Manifest.PostgreSQL (

    PostgreSQL

  , postgresql
  , ConnectInfo(..)

  -- TBD hide constructors, use pattern synonyms?
  , PostgreSQLManifestFailure(..)

  ) where

import qualified Data.Text as T
import qualified Data.ByteString as BS
import qualified Data.ByteString.Char8 as B8
import Data.String (fromString)
import Control.Applicative
import Control.Exception
import Control.Monad.IO.Class
import Control.Monad.Trans.Class
import Control.Monad.Trans.Reader
import Control.Monad.Trans.Except
import Database.PostgreSQL.Simple
import Data.TypeNat.Vect
import Data.Proxy
import Manifest.Manifest

type TableName = BS.ByteString

-- | TODO state assumptions on the db form.
data PostgreSQL a = PostgreSQL {
    postgresInfo :: ConnectInfo
  , tableName :: TableName
  }

postgresql :: ConnectInfo -> TableName -> PostgreSQL a
postgresql = PostgreSQL

data PostgreSQLManifestFailure
  = PostgreSQLManifestNoConnection
  -- ^ Could not get a connection.
  | PostgreSQLManifestReadFailure
  -- ^ Could not translate SQL row to datatype.
  | PostgreSQLManifestWriteFailure
  -- ^ Could not translate datatype to SQL row.
  | PostgreSQLManifestDeleteFailure
  | PostgreSQLManifestTransactionFailure
  -- ^ Transaction failed. May want to try again.
  | PostgreSQLManifestOtherFailure
  -- ^ Something else went wrong; not sure what.
  deriving (Show)

-- | We just need this guy for the vectSQLSelectQuery function, which uses
--   natRecursion from IsNat, and so must work on some datatype which carries
--   a Nat index.
newtype BSSWithNatProxy (n :: Nat) = BSSWNP {
    unBSSWNP :: [BS.ByteString]
  }

-- | Dump a BSSWithNatProxy to a string which can be used as a select clause
--   in an SQL query.
exitBSSWNP :: BSSWithNatProxy n -> BS.ByteString
exitBSSWNP = BS.intercalate "," . unBSSWNP

-- | Build a BSSWithNatProxy n and then exit it via exitBSSWNP so as to produce
--   a suitable SELECT query.
vectSQLSelectQuery :: IsNat n => u n -> TableName -> Query
vectSQLSelectQuery proxyN tableName = fromString . B8.unpack . BS.concat $
    ["SELECT ", exitBSSWNP (vectSQLSelectQuery' proxyN), " FROM ", tableName, " WHERE key=?"]

  where

    vectSQLSelectQuery' :: forall u n . IsNat n => u n -> BSSWithNatProxy n
    vectSQLSelectQuery' _ = natRecursion inductive base incr 1

      where

        incr :: Int -> Int
        incr = (+) 1

        base _ = BSSWNP []

        -- Careful to surround those numbers in quotes! It's essential for
        -- correct interpretation by PostgreSQL.
        inductive n (BSSWNP bss) = BSSWNP $
          BS.concat ["\"", (B8.pack (show n)), "\""] : bss

-- | Produce a suitable UPDATE query.
--   Give a vector of values (not including the key).
--   Resulting query should be executed with the key at the end of the vector
--   of values.
vectSQLUpdateQuery :: IsNat n => Vect a n -> TableName -> Query
vectSQLUpdateQuery v tableName = fromString . B8.unpack . BS.concat $
    ["UPDATE ", tableName, " SET ", exitBSSWNP (vectSQLUpdateQuery' v), " WHERE key=?"]

  where

    vectSQLUpdateQuery' :: IsNat n => Vect a n -> BSSWithNatProxy n
    vectSQLUpdateQuery' _ = natRecursion inductive base incr 1

      where

        incr :: Int -> Int
        incr = (+) 1

        base _ = BSSWNP []

        inductive n (BSSWNP bss) = BSSWNP $
          BS.concat ["\"", (B8.pack (show n)), "\"=?"] : bss

-- | Produce a suitable INSERT query.
--   Here we don't need the IsNat class's natRecursion because we already have
--   a Vect in hand, whereas in vectSQLSelectQuery we have only type
--   information (description of the nat as a type).
--   Must give a Vect whose length is the number of columns desired.
--   BUT the resulting query will have n+1 question marks; you should use
--   the key as the last one.
vectSQLInsertQuery :: Vect a n -> TableName -> Query
vectSQLInsertQuery v tableName = fromString . B8.unpack . BS.concat $
    [ "INSERT INTO "
    , tableName
    , " SELECT "
    , vectSQLInsertQuery' v
    , " WHERE NOT EXISTS (SELECT * FROM "
    , tableName
    , " WHERE key=?)"
    ]

  where

    vectSQLInsertQuery' :: Vect a n -> BS.ByteString
    vectSQLInsertQuery' vect = case vect of
      VNil -> ""
      VCons _ VNil -> "?"
      VCons _ v -> BS.append "?," (vectSQLInsertQuery' v)

-- | PostgreSQL manifest.
instance Manifest PostgreSQL where

  type ManifestMonad PostgreSQL =
      ReaderT (Connection, TableName) (ExceptT PostgreSQLManifestFailure IO)
  type PeculiarManifestFailure PostgreSQL = PostgreSQLManifestFailure

  manifestRead proxy (proxy' :: u n) key = do
      (conn, tableName) <- ask
      let queryString = vectSQLSelectQuery proxy' tableName
      let binaryKey = Binary key
      -- Query won't indicate a failure to read via a Maybe, it will just
      -- throw an exception :( We work hard to catch it here.
      rows <- liftIO $
          catchJust
          (queryErrorSelector)
          (Right <$> (query conn queryString (Only binaryKey) :: IO [Vect BS.ByteString n]))
          (catchQueryErrors)
      case rows of
        Left failure -> lift $ throwE failure
        Right [] -> return Nothing
        Right [x] -> return $ Just x
        Right (x : _) -> return $ Just x
        -- ^ TBD print warning here??

    where

      queryErrorSelector :: SomeException -> Maybe PostgreSQLManifestFailure
      queryErrorSelector e =
              (const PostgreSQLManifestOtherFailure <$> isSqlError e)
          <|> (const PostgreSQLManifestReadFailure <$> isResultError e)

        where

          isSqlError :: SomeException -> Maybe SqlError
          isSqlError = fromException

          isResultError :: SomeException -> Maybe ResultError
          isResultError = fromException

      catchQueryErrors :: PostgreSQLManifestFailure -> IO (Either PostgreSQLManifestFailure a)
      catchQueryErrors = return . Left

  manifestWrite proxy proxy' key valueVect = do
      (conn, tableName) <- ask
      -- Here we just make Query types and juggle the Vects so as to
      -- provide the correct number of arguments in the correct places.
      let valueAndKey = vectSnoc key valueVect
      let keyAndValue = VCons key valueVect
      let keysAndValue = vectSnoc key keyAndValue
      let binaryValueAndKey = vectMap Binary valueAndKey
      let binaryKeysAndValue = vectMap Binary keysAndValue
      let updateQueryString = vectSQLUpdateQuery valueVect tableName
      let insertQueryString = vectSQLInsertQuery keyAndValue tableName
      let upsert = do
            execute conn updateQueryString binaryValueAndKey
            execute conn insertQueryString binaryKeysAndValue
      result <- liftIO $
          catchJust
          (executeErrorSelector)
          (Right <$> upsert)
          (catchExecuteErrors)
      case result of
        Left failure -> lift $ throwE failure
        Right _ -> return ()

    where

      executeErrorSelector :: SomeException -> Maybe PostgreSQLManifestFailure
      executeErrorSelector e =
          (const PostgreSQLManifestWriteFailure <$> isSqlError e)

        where

          isSqlError :: SomeException -> Maybe SqlError
          isSqlError = fromException

      catchExecuteErrors :: PostgreSQLManifestFailure -> IO (Either PostgreSQLManifestFailure a)
      catchExecuteErrors = return . Left

  manifestDelete proxy key = do
      (conn, tableName) <- ask
      let queryString = fromString (B8.unpack (BS.concat ["DELETE FROM ", tableName, " WHERE key=?"]))
      let binaryKey = Binary key
      result <- liftIO $
          catchJust
          (executeErrorSelector)
          (Right <$> execute conn queryString (Only binaryKey))
          (catchExecuteErrors)
      case result of
        Left failure -> lift $ throwE failure
        Right _ -> return ()

    where

      executeErrorSelector :: SomeException -> Maybe PostgreSQLManifestFailure
      executeErrorSelector e =
          (const PostgreSQLManifestDeleteFailure <$> isSqlError e)

        where

          isSqlError :: SomeException -> Maybe SqlError
          isSqlError = fromException

      catchExecuteErrors :: PostgreSQLManifestFailure -> IO (Either PostgreSQLManifestFailure a)
      catchExecuteErrors = return . Left

  manifestRun p@(PostgreSQL connInfo tableName) action = do
      eitherConn :: Either SomeException Connection <- try (connect connInfo)
      case eitherConn of
        Left _ -> return (Left PostgreSQLManifestNoConnection, p)
        Right conn -> do
          let exceptTAction = runReaderT action (conn, tableName)
          -- Begin transaction
          begin conn
          outcome <- runExceptT exceptTAction
          case outcome of
            Left failure -> catch (rollback conn >> return (Left failure, p)) (rollbackCatch p)
            Right value -> catch (commit conn >> return (Right value, p)) (commitCatch p)

    where

      rollbackCatch :: PostgreSQL b -> SomeException -> IO (Either PostgreSQLManifestFailure a, PostgreSQL b)
      rollbackCatch p _ = return (Left PostgreSQLManifestTransactionFailure, p)

      commitCatch :: PostgreSQL b -> SomeException -> IO (Either PostgreSQLManifestFailure a, PostgreSQL b)
      commitCatch p _ = return (Left PostgreSQLManifestTransactionFailure, p)
