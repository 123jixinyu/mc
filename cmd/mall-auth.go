// Copyright (c) 2015-2024 MinIO, Inc.
//
// This file is part of MinIO Object Storage stack
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

package cmd

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/minio/cli"
	"github.com/minio/pkg/v2/console"
	"io"
	"net/http"
	"os"
	"strings"
	"time"
)

const (
	DefaultServerEndpoint = "http://localhost:9000"
	AuthStoreFileName     = "auth"
	AuthAlias             = "gpumall"
)

// auth command flags.
var (
	authFlags = []cli.Flag{
		cli.StringFlag{
			Name:  "region",
			Usage: "Set region,default region is sh-01",
			Value: "sh-01",
		},
		cli.StringFlag{
			Name:  "user",
			Usage: "Set auth user",
		},
		cli.StringFlag{
			Name:  "password",
			Usage: "Your auth password",
		},
	}
)

// Get command.
var authCmd = cli.Command{
	Name:         "auth",
	Usage:        "Auth to gpumall.com",
	Action:       mainAuth,
	OnUsageError: onUsageError,
	Before:       setGlobalsFromContext,
	Flags:        append(globalFlags, authFlags...),
	CustomHelpTemplate: `NAME:
  {{.HelpName}} - {{.Usage}}

USAGE:
  {{.HelpName}} [FLAGS] SOURCE TARGET

FLAGS:
  {{range .VisibleFlags}}{{.}}
  {{end}}

EXAMPLES:
  1. auth to gpumall.com
    {{.Prompt}} {{.HelpName}} --region sh-01 --user=foo --password=12456
`,
}

// mainAuth is the entry point for auth command.
func mainAuth(cliCtx *cli.Context) (e error) {

	region := strings.TrimSpace(cliCtx.String("region"))
	if region == "" {
		return errors.New("Please enter regison  by use '--region'")
	}
	user := strings.TrimSpace(cliCtx.String("user"))
	if user == "" {
		return errors.New("Please enter auth user by use '--user'")
	}
	password := strings.TrimSpace(cliCtx.String("password"))
	if password == "" {
		return errors.New("Please enter auth password by use '--password'")
	}

	authData, err := auth(region, user, password)
	if err != nil {
		if globalDebug {
			console.Errorln(err)
		}
		return errors.New("Auth failed")
	}
	if err := storeAuthData(AuthStoreFileName, authData.Data); err != nil {
		return err
	}

	return nil
}

// get minio access info from gpumall.com
func auth(region string, user string, password string) (AuthInfoResponse, error) {

	var authRes AuthInfoResponse

	authUrl := serverEndpoint() + "/api/v1/auth/cli/login"

	params := map[string]interface{}{
		"phone":    user,
		"password": password,
		"dcId":     region,
	}
	p, _ := json.Marshal(params)

	r, err := http.Post(authUrl, "application/json", bytes.NewBuffer(p))
	if err != nil {
		return authRes, errors.New(fmt.Sprintf("Auth to gpumall.com failed: %s", err.Error()))
	}

	defer r.Body.Close()

	body, err := io.ReadAll(r.Body)
	if err != nil {
		return authRes, errors.New(fmt.Sprintf("Read auth server response failed: %s", err.Error()))
	}
	err = json.Unmarshal(body, &authRes)
	if err != nil {
		return authRes, err
	}
	if authRes.Code == 0 && authRes.Message == "success" {
		fmt.Println("Auth successful")
		return authRes, nil
	}
	return authRes, errors.New(fmt.Sprintf("Auth  failed: %s", authRes.Message))
}

// store auth data
func storeAuthData(sId string, v interface{}) error {

	sessionDataFile, errFile := getSessionDataFile(sId)
	if errFile != nil {
		return errors.New("Unable to create session data file")
	}
	_, err := os.Create(sessionDataFile)
	if err != nil {
		return errors.New("Unable to create session data file")
	}
	s, err := json.Marshal(v)
	if err != nil {
		return errors.New("Unable to marshal session data")
	}
	err = os.WriteFile(sessionDataFile, s, 0644)
	if err != nil {
		return errors.New("Write session data failed")
	}
	return nil
}

// get auth data
func getAuthWithErr() (AuthData, error) {

	var authData AuthData
	df, pErr := getSessionDataFile(AuthStoreFileName)
	if pErr != nil {
		return authData, pErr.ToGoError()
	}
	f, err := os.Open(df)
	if err != nil {
		return authData, err
	}
	defer f.Close()

	content, err := io.ReadAll(f)
	if err != nil {
		return authData, errors.New(fmt.Sprintf("Read session failed:%v", err))
	}
	if err := json.Unmarshal(content, &authData); err != nil {
		return authData, err
	}

	expireAt, err := time.Parse("2006-01-02 15:04:05", authData.ExpireAt)
	if err != nil {
		return authData, errors.New(fmt.Sprintf("Get session data expiredAt failed:%v", err))
	}

	if time.Now().After(expireAt) {
		return authData, errors.New(fmt.Sprintf("Token has expired, please reauthorize"))
	}

	return authData, nil
}

func getAuth() AuthData {

	auth, err := getAuthWithErr()
	if err != nil {
		if globalDebug {
			fmt.Println(err)
		}
		console.Fatalln("Auth failed, please reauthorize")
	}
	return auth
}

type AuthInfoResponse struct {
	Code    int      `json:"code"`
	Message string   `json:"message"`
	TraceId string   `json:"traceid"`
	Data    AuthData `json:"data"`
}

type AuthData struct {
	Endpoint     string `json:"endpoint" dc:"网盘访问地址"`
	BasePath     string `json:"basePath" dc:"访问根目录"`
	Bucket       string `json:"bucket" dc:"bucket"`
	AccessKey    string `json:"accessKey" dc:"accessKey"`
	SecretKey    string `json:"secretKey" dc:"secretKey"`
	SessionToken string `json:"sessionToken" dc:"sessionToken"`
	ExpireAt     string `json:"expireAt" dc:"expireAt"`
}

// get gpumall.com server address
func serverEndpoint() string {

	gpuMallServer := os.Getenv("GPU_MALL_SERVER")
	if gpuMallServer != "" {
		return gpuMallServer
	}
	return DefaultServerEndpoint
}
