#!/usr/bin/env bash

PASSWORD="123456"

# Generate the CA
cfssl genkey -initca ca.json | cfssljson -bare ca

# Generate server keys
cfssl gencert -ca ca.pem -ca-key ca-key.pem server-0.json | cfssljson -bare server-0
cfssl gencert -ca ca.pem -ca-key ca-key.pem server-1.json | cfssljson -bare server-1
cfssl gencert -ca ca.pem -ca-key ca-key.pem server-2.json | cfssljson -bare server-2

# Generate user keys
cfssl gencert -ca ca.pem -ca-key ca-key.pem user1.json | cfssljson -bare user1
cfssl gencert -ca ca.pem -ca-key ca-key.pem user2.json | cfssljson -bare user2

# Convert CA to Java Keystore format (truststrore)
rm truststore
keytool -importcert -keystore truststore -storepass $PASSWORD -storetype JKS -alias ca -file ca.pem -noprompt

# Convert keys to PKCS12
openssl pkcs12 -export -out server-0.p12 -in server-0.pem -inkey server-0-key.pem -password pass:$PASSWORD
openssl pkcs12 -export -out server-1.p12 -in server-1.pem -inkey server-1-key.pem -password pass:$PASSWORD
openssl pkcs12 -export -out server-2.p12 -in server-2.pem -inkey server-2-key.pem -password pass:$PASSWORD
openssl pkcs12 -export -out user1.p12 -in user1.pem -inkey user1-key.pem -password pass:$PASSWORD
openssl pkcs12 -export -out user2.p12 -in user2.pem -inkey user2-key.pem -password pass:$PASSWORD

# Convert PKCS12 keys to keystores
rm *.keystore
keytool -importkeystore -srckeystore server-0.p12 -srcstoretype PKCS12 -srcstorepass $PASSWORD -destkeystore server-0.keystore -deststoretype JKS -deststorepass $PASSWORD -noprompt
keytool -importkeystore -srckeystore server-1.p12 -srcstoretype PKCS12 -srcstorepass $PASSWORD -destkeystore server-1.keystore -deststoretype JKS -deststorepass $PASSWORD -noprompt
keytool -importkeystore -srckeystore server-2.p12 -srcstoretype PKCS12 -srcstorepass $PASSWORD -destkeystore server-2.keystore -deststoretype JKS -deststorepass $PASSWORD -noprompt
keytool -importkeystore -srckeystore user1.p12 -srcstoretype PKCS12 -srcstorepass $PASSWORD -destkeystore user1.keystore -deststoretype JKS -deststorepass $PASSWORD -noprompt
keytool -importkeystore -srckeystore user2.p12 -srcstoretype PKCS12 -srcstorepass $PASSWORD -destkeystore user2.keystore -deststoretype JKS -deststorepass $PASSWORD -noprompt