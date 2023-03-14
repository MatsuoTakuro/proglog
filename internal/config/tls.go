package config

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
)

type TLSConfig struct {
	CertFile      string
	KeyFile       string
	CAFile        string
	ServerAddress string
	IsServer      bool
}

func SetupTLSConfig(cfg TLSConfig) (*tls.Config, error) {
	var err error
	// set a minimum version of TLS
	tlsConfig := &tls.Config{MinVersion: tls.VersionTLS13}

	if cfg.CertFile != "" && cfg.KeyFile != "" {
		// create only one certificate
		// set a public/private key pair (pem files) in it
		tlsConfig.Certificates = make([]tls.Certificate, 1)
		tlsConfig.Certificates[0], err = tls.LoadX509KeyPair(
			cfg.CertFile,
			cfg.KeyFile,
		)
		if err != nil {
			return nil, err
		}
	}

	// if you have a private key (a pem file) for your own CA (certification authority)
	if cfg.CAFile != "" {
		b, err := os.ReadFile(cfg.CAFile)
		if err != nil {
			return nil, err
		}
		ca := x509.NewCertPool()
		ok := ca.AppendCertsFromPEM([]byte(b))
		if !ok {
			return nil, fmt.Errorf(
				"failed to parse root certificate: %q",
				cfg.CAFile,
			)
		}

		if cfg.IsServer {
			// set CA for server
			// server requests client certificate from client
			tlsConfig.ClientCAs = ca
			tlsConfig.ClientAuth = tls.RequireAndVerifyClientCert
		} else {
			// set CA for root-client/nobody-client
			tlsConfig.RootCAs = ca
		}
		// set hosts that can be authenticated
		tlsConfig.ServerName = cfg.ServerAddress
	}
	return tlsConfig, nil
}
