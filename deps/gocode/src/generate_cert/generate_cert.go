// @author Couchbase <info@couchbase.com>
// @copyright 2015 Couchbase, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package main

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"flag"
	"fmt"
	"hash/crc32"
	"io"
	"log"
	"math/big"
	"net"
	"os"
	"time"
)

func mustNoErr(err error) {
	if err != nil {
		log.Fatal(err)
	}
}

var earlyNotBefore = time.Date(2013, 1, 1, 0, 0, 0, 0, time.UTC)

// that's max date that current golang x509 code supports
var earlyNotAfter = time.Date(2049, 12, 31, 23, 59, 59, 0, time.UTC)

func pemIfy(octets []byte, pemType string, out io.Writer) {
	pem.Encode(out, &pem.Block{
		Type:  pemType,
		Bytes: octets,
	})
}

func derToPKey(octets []byte) (pkey *rsa.PrivateKey) {
	pkey, err := x509.ParsePKCS1PrivateKey(octets)
	if err == nil {
		return
	}

	pkeyInt, err2 := x509.ParsePKCS8PrivateKey(octets)
	pkey, rsaPKey := pkeyInt.(*rsa.PrivateKey)
	if err2 == nil && !rsaPKey {
		err2 = errors.New("only rsa keys are supported yet")
	}
	if err2 == nil {
		return
	}

	log.Printf("Failed to parse pkey: %s\nOther error is:", err)
	log.Fatal(err2)
	panic("cannot happen")
}

var keyLength = 2048

func init() {
	if os.Getenv("COUCHBASE_SMALLER_PKEYS") == "1" {
		keyLength = 1024
	}
}

func main() {
	var genereateLeaf bool
	var commonName string
	var useSha1 bool

	flag.StringVar(&commonName, "common-name", "*", "common name field of certificate (hostname)")
	flag.BoolVar(&genereateLeaf, "generate-leaf", false, "whether to generate leaf certificat (passing ca cert and pkey via environment variables)")

	flag.BoolVar(&useSha1, "use-sha1", false, "whether to use sha1 instead of default sha256 signature algorithm")

	flag.Parse()

	if genereateLeaf {
		cacertPEM := os.Getenv("CACERT")
		certBlock, rest := pem.Decode(([]byte)(cacertPEM))
		if (string)(rest) != "" || certBlock == nil || certBlock.Type != "CERTIFICATE" {
			log.Fatal("garbage CACERT environment variable")
		}

		capkeyPEM := os.Getenv("CAPKEY")
		pkeyBlock, rest := pem.Decode(([]byte)(capkeyPEM))
		// TODO: support EC keys too, which might be useful sometimes
		if (string)(rest) != "" || pkeyBlock == nil || pkeyBlock.Type != "RSA PRIVATE KEY" {
			log.Fatal("garbage CAPKEY environment variable")
		}

		caCert, err := x509.ParseCertificate(certBlock.Bytes)
		mustNoErr(err)

		pkey := derToPKey(pkeyBlock.Bytes)

		leafPKey, err := rsa.GenerateKey(rand.Reader, keyLength)
		mustNoErr(err)

		leafTemplate := x509.Certificate{
			SerialNumber: big.NewInt(time.Now().UnixNano()),
			NotBefore:    earlyNotBefore,
			NotAfter:     earlyNotAfter,
			Subject: pkix.Name{
				CommonName: commonName,
			},
			KeyUsage:           x509.KeyUsageKeyEncipherment | x509.KeyUsageDigitalSignature,
			SignatureAlgorithm: caCert.SignatureAlgorithm,
			ExtKeyUsage: []x509.ExtKeyUsage{
				x509.ExtKeyUsageServerAuth,
				x509.ExtKeyUsageClientAuth,
			},
			BasicConstraintsValid: true,
		}

		if ip := net.ParseIP(commonName); ip != nil {
			leafTemplate.IPAddresses = []net.IP{ip}
		}

		certDer, err := x509.CreateCertificate(rand.Reader, &leafTemplate, caCert, &leafPKey.PublicKey, pkey)
		mustNoErr(err)

		pemIfy(certDer, "CERTIFICATE", os.Stdout)
		pemIfy(x509.MarshalPKCS1PrivateKey(leafPKey), "RSA PRIVATE KEY", os.Stdout)
	} else {
		pkey, err := rsa.GenerateKey(rand.Reader, keyLength)
		mustNoErr(err)

		commonName = fmt.Sprintf("Couchbase Server %08x", crc32.ChecksumIEEE(pkey.N.Bytes()))

		template := x509.Certificate{
			SerialNumber: big.NewInt(time.Now().UnixNano()),
			IsCA:         true,
			NotBefore:    earlyNotBefore,
			NotAfter:     earlyNotAfter,
			Subject: pkix.Name{
				CommonName: commonName,
			},
			KeyUsage: x509.KeyUsageKeyEncipherment | x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
			ExtKeyUsage: []x509.ExtKeyUsage{
				x509.ExtKeyUsageServerAuth,
				x509.ExtKeyUsageClientAuth,
			},
			BasicConstraintsValid: true,
		}

		if useSha1 {
			template.SignatureAlgorithm = x509.SHA1WithRSA
		}

		certDer, err := x509.CreateCertificate(rand.Reader, &template, &template, &pkey.PublicKey, pkey)
		mustNoErr(err)

		pemIfy(certDer, "CERTIFICATE", os.Stdout)
		pemIfy(x509.MarshalPKCS1PrivateKey(pkey), "RSA PRIVATE KEY", os.Stdout)
	}
}
