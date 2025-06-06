// The MIT License
//
// Copyright (c) 2025 Manetu Inc.  All rights reserved.
// Copyright (c) 2020 Temporal Technologies Inc.  All rights reserved.
//
// Copyright (c) 2020 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package config

import (
	"fmt"
	"os"
	"reflect"
	"strings"
	"time"

	"github.com/yugabyte/gocql"
	"go.temporal.io/server/common/auth"
	"go.temporal.io/server/common/config"
)

type (

	// Yugabyte contains configuration to connect to Yugabyte cluster
	Yugabyte struct {
		// Hosts is a csv of Yugabyte endpoints
		Hosts string `yaml:"hosts" validate:"nonzero"`
		// Port is the Yugabyte port used for connection by gocql client
		Port int `yaml:"port"`
		// User is the Yugabyte user used for authentication by gocql client
		User string `yaml:"user"`
		// Password is the Yugabyte password used for authentication by gocql client
		Password string `yaml:"password"`
		// AllowedAuthenticators is the optional list of authenticators the gocql client checks before approving the challenge request from the server.
		AllowedAuthenticators []string `yaml:"allowedAuthenticators"`
		// keyspace is the Yugabyte keyspace
		Keyspace string `yaml:"keyspace" validate:"nonzero"`
		// Datacenter is the data center filter arg for Yugabyte
		Datacenter string `yaml:"datacenter"`
		// MaxConns is the max number of connections to this datastore for a single keyspace
		MaxConns int `yaml:"maxConns"`
		// ConnectTimeout is a timeout for initial dial to Yugabyte server (default: 600 milliseconds)
		ConnectTimeout time.Duration `yaml:"connectTimeout"`
		// Timeout is a timeout for reads and, unless otherwise specified, writes. If not specified, ConnectTimeout is used.
		Timeout time.Duration `yaml:"timeout"`
		// WriteTimeout is a timeout for writing a query. If not specified, Timeout is used.
		WriteTimeout time.Duration `yaml:"writeTimeout"`
		// TLS configuration
		TLS *auth.TLS `yaml:"tls"`
		// Consistency configuration (defaults to LOCAL_QUORUM / LOCAL_SERIAL for all stores if this field not set)
		Consistency *YugabyteStoreConsistency `yaml:"consistency"`
		// DisableInitialHostLookup instructs the gocql client to connect only using the supplied hosts
		DisableInitialHostLookup bool `yaml:"disableInitialHostLookup"`
		// AddressTranslator translates Yugabyte IP addresses, used for cases when IP addresses gocql driver returns are not accessible from the server
		AddressTranslator *YugabyteAddressTranslator `yaml:"addressTranslator"`
	}

	// YugabyteStoreConsistency enables you to set the consistency settings for each Yugabyte Persistence Store for Temporal
	YugabyteStoreConsistency struct {
		// Default defines the consistency level for ALL stores.
		// Defaults to LOCAL_QUORUM and LOCAL_SERIAL if not set
		Default *YugabyteConsistencySettings `yaml:"default"`
	}

	YugabyteAddressTranslator struct {
		// Translator defines name of translator implementation to use for Yugabyte address translation
		Translator string `yaml:"translator"`
		// Options map of options for address translator implementation
		Options map[string]string `yaml:"options"`
	}

	// YugabyteConsistencySettings sets the default consistency level for regular & serial queries to Yugabyte.
	YugabyteConsistencySettings struct {
		// Consistency sets the default consistency level. Values identical to gocql Consistency values. (defaults to LOCAL_QUORUM if not set).
		Consistency string `yaml:"consistency"`
		// SerialConsistency sets the consistency for the serial prtion of queries. Values identical to gocql SerialConsistency values. (defaults to LOCAL_SERIAL if not set)
		SerialConsistency string `yaml:"serialConsistency"`
	}
)

// GetConsistency returns the gosql.Consistency setting from the configuration for the given store type
func (c *YugabyteStoreConsistency) GetConsistency() gocql.Consistency {
	return gocql.ParseConsistency(c.getConsistencySettings().Consistency)
}

// GetSerialConsistency returns the gosql.SerialConsistency setting from the configuration for the store
func (c *YugabyteStoreConsistency) GetSerialConsistency() gocql.SerialConsistency {
	res, err := parseSerialConsistency(c.getConsistencySettings().SerialConsistency)
	if err != nil {
		panic(fmt.Sprintf("unable to decode driver serial consistency: %v", err))
	}
	return res
}

func (c *YugabyteStoreConsistency) getConsistencySettings() *YugabyteConsistencySettings {
	return ensureStoreConsistencyNotNil(c).Default
}

func ensureStoreConsistencyNotNil(c *YugabyteStoreConsistency) *YugabyteStoreConsistency {
	if c == nil {
		c = &YugabyteStoreConsistency{}
	}
	if c.Default == nil {
		c.Default = &YugabyteConsistencySettings{}
	}
	if c.Default.Consistency == "" {
		c.Default.Consistency = "LOCAL_QUORUM"
	}
	if c.Default.SerialConsistency == "" {
		c.Default.SerialConsistency = "LOCAL_SERIAL"
	}

	return c
}

func (c *Yugabyte) validate() error {
	return c.Consistency.validate()
}

func (c *YugabyteStoreConsistency) validate() error {
	if c == nil {
		return nil
	}

	v := reflect.ValueOf(*c)

	for i := 0; i < v.NumField(); i++ {
		s, ok := v.Field(i).Interface().(*YugabyteConsistencySettings)
		if ok {
			if err := s.validate(); err != nil {
				return err
			}
		}
	}

	return nil
}

func (c *YugabyteConsistencySettings) validate() error {
	if c == nil {
		return nil
	}

	if c.Consistency != "" {
		_, err := gocql.ParseConsistencyWrapper(c.Consistency)
		if err != nil {
			return fmt.Errorf("bad driver consistency: %v", err)
		}
	}

	if c.SerialConsistency != "" {
		_, err := parseSerialConsistency(c.SerialConsistency)
		if err != nil {
			return fmt.Errorf("bad driver serial consistency: %v", err)
		}
	}

	return nil
}

func parseSerialConsistency(serialConsistency string) (gocql.SerialConsistency, error) {
	var s gocql.SerialConsistency
	err := s.UnmarshalText([]byte(strings.ToUpper(serialConsistency)))
	return s, err
}

func importTls(cfg config.CustomDatastoreConfig) *auth.TLS {
	tls := &auth.TLS{}

	options, ok := cfg.Options["tls"].(map[string]interface{})
	if !ok {
		return tls
	}

	if enabled, ok := options["enabled"].(bool); ok {
		tls.Enabled = enabled
	}

	if enableHostVerification, ok := options["enableHostVerification"].(bool); ok {
		tls.EnableHostVerification = enableHostVerification
	}
	if serverName, ok := options["serverName"].(string); ok {
		tls.ServerName = serverName
	}

	if caFile, ok := options["caFile"].(string); ok {
		tls.CaFile = caFile
	}
	if caData, ok := options["caData"].(string); ok {
		tls.CaData = caData
	}

	if certFile, ok := options["certFile"].(string); ok {
		tls.CertFile = certFile
	}
	if certData, ok := options["certData"].(string); ok {
		tls.CertData = certData
	}

	if keyFile, ok := options["keyFile"].(string); ok {
		tls.KeyFile = keyFile
	}
	if keyData, ok := options["keyData"].(string); ok {
		tls.KeyData = keyData
	}

	return tls
}

func ImportConfig(cfg config.CustomDatastoreConfig) (Yugabyte, error) {
	config := Yugabyte{
		Hosts:    cfg.Options["hosts"].(string),
		Keyspace: cfg.Options["keyspace"].(string),
		TLS:      importTls(cfg),
	}
	if datacenter, ok := cfg.Options["datacenter"].(string); ok {
		config.Datacenter = datacenter
	}
	if user, ok := cfg.Options["user"].(string); ok {
		config.User = user
		config.Password = os.Getenv("YUGABYTE_PASSWORD")
	}

	return config, nil
}
