{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE OverloadedStrings #-}

import qualified Data.ByteString as BS
import Control.Applicative ((<$>))
import Data.Proxy
import Manifest.Manifest
import Manifest.PureManifest
import Manifest.PostgreSQL

data User = User BS.ByteString
  deriving (Show)

type Email = BS.ByteString

data WithEmail a = WithEmail a Email
  deriving (Show)

email :: WithEmail a -> Email
email (WithEmail _ x) = x

instance ManifestKey User where
  manifestibleKeyDump (User b) = b
  manifestibleKeyPull = Just . User

instance Manifestible (WithEmail a) where
  type ManifestibleKey (WithEmail a) = a
  type ManifestibleValue (WithEmail a) = Email
  manifestibleKey (WithEmail x _) = x
  manifestibleValue (WithEmail _ x) = x
  manifestibleFactorization = WithEmail

ada = User "ada"
richard = User "richard"
adaEmail = WithEmail ada "ada@clare.com"
richardEmail = WithEmail richard "richard@carstone.com"

-- If we wish to use a PureManifest, all we have to do is uncomment this,
-- and remove the conflicting definitions below!
--type MyEmailManifest = PureManifest
--myEmailManifest :: MyEmailManifest a
--myEmailManifest = emptyPM

type MyEmailManifest = PostgreSQL
myEmailManifest = postgresql info "emails"
  where
    info = ConnectInfo "localhost" (fromIntegral 5432) "" "" "alex"

main = do
    let emailManifest = myEmailManifest
    let writeem = do {
        mput adaEmail (Proxy :: Proxy MyEmailManifest);
        mput richardEmail  (Proxy :: Proxy MyEmailManifest);
      }
    let readem = do {
        adasEmail <- mget ada (Proxy :: Proxy (MyEmailManifest (WithEmail User)));
        richardsEmail <- mget richard (Proxy :: Proxy (MyEmailManifest (WithEmail User)));
        return (email <$> adasEmail, email <$> richardsEmail)
      }
    (writeOutcome, emailManifest) <- manifest emailManifest writeem
    (readOutcome1, emailManifest) <- manifest emailManifest readem
    let deleteem = do {
        richardsEmail <- mdel richard (Proxy :: Proxy (MyEmailManifest (WithEmail User)));
        return richardsEmail
      }
    (deleteOutcome, emailManifest) <- manifest emailManifest deleteem
    (readOutcome2, emailManifest) <- manifest emailManifest readem
    printOutcome writeOutcome
    printOutcome readOutcome1
    printOutcome deleteOutcome
    printOutcome readOutcome2

  where

    printOutcome x = case x of
      Right y -> putStr "OK! " >> print y
      Left y -> case y of
        GeneralFailure f -> putStr "Not OK! " >> print f
        PeculiarFailure f -> putStr "Not OK! " >> print f
