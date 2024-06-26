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
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/dustin/go-humanize"
	"github.com/minio/cli"
	"github.com/minio/mc/pkg/probe"
	"github.com/minio/pkg/v2/console"
)

// put command flags.
var (
	putFlags = []cli.Flag{
		cli.IntFlag{
			Name:  "parallel, P",
			Usage: "upload number of parts in parallel",
			Value: 4,
		},
		cli.StringFlag{
			Name:  "part-size, s",
			Usage: "each part size",
			Value: "16MiB",
		},
	}
)

// Put command.
var putCmd = cli.Command{
	Name:         "put",
	Usage:        "upload an object to a bucket",
	Action:       mainPut,
	OnUsageError: onUsageError,
	Before:       setGlobalsFromContext,
	Flags:        append(append(ioFlags, globalFlags...), putFlags...),
	CustomHelpTemplate: `NAME:
  {{.HelpName}} - {{.Usage}}

USAGE:
  {{.HelpName}} [FLAGS] SOURCE TARGET

FLAGS:
  {{range .VisibleFlags}}{{.}}
  {{end}}
ENVIRONMENT VARIABLES:
  MC_ENCRYPT:      list of comma delimited prefixes
  MC_ENCRYPT_KEY:  list of comma delimited prefix=secret values

EXAMPLES:
  1. Put an object from local file system to S3 storage
    {{.Prompt}} {{.HelpName}} path-to/object ALIAS/BUCKET
  2. Put an object from local file system to S3 bucket with name
    {{.Prompt}} {{.HelpName}} path-to/object ALIAS/BUCKET/OBJECT-NAME
  3. Put an object from local file system to S3 bucket under a prefix
    {{.Prompt}} {{.HelpName}} path-to/object ALIAS/BUCKET/PREFIX/
`,
}

// mainPut is the entry point for put command.
func mainPut(cliCtx *cli.Context) (e error) {
	args := cliCtx.Args()
	if len(args) < 2 {
		showCommandHelpAndExit(cliCtx, 1) // last argument is exit code.
	}

	ctx, cancelPut := context.WithCancel(globalContext)
	defer cancelPut()
	// part size
	size := cliCtx.String("s")
	if size == "" {
		size = "16mb"
	}
	_, perr := humanize.ParseBytes(size)
	if perr != nil {
		fatalIf(probe.NewError(perr), "Unable to parse part size")
	}
	// threads
	threads := cliCtx.Int("P")
	if threads < 1 {
		fatalIf(errInvalidArgument().Trace(strconv.Itoa(threads)), "Invalid number of threads")
	}

	encKeyDB, err := getEncKeys(cliCtx)
	fatalIf(err, "Unable to parse encryption keys.")

	if len(args) < 2 {
		fatalIf(errInvalidArgument().Trace(args...), "Invalid number of arguments.")
	}
	// get source and target
	sourceURLs := args[:len(args)-1]
	targetURL := getFullPath(args[len(args)-1])

	fmt.Println(targetURL)

	putURLsCh := make(chan URLs, 10000)
	var totalObjects, totalBytes int64

	// Store a progress bar or an accounter
	var pg ProgressReader

	// Enable progress bar reader only during default mode.
	if !globalQuiet && !globalJSON { // set up progress bar
		pg = newProgressBar(totalBytes)
	} else {
		pg = newAccounter(totalBytes)
	}
	go func() {
		opts := prepareCopyURLsOpts{
			sourceURLs:              sourceURLs,
			targetURL:               targetURL,
			encKeyDB:                encKeyDB,
			ignoreBucketExistsCheck: true,
		}

		for putURLs := range preparePutURLs(ctx, opts) {
			if putURLs.Error != nil {
				putURLsCh <- putURLs
				break
			}
			totalBytes += putURLs.SourceContent.Size
			pg.SetTotal(totalBytes)
			totalObjects++
			putURLsCh <- putURLs
		}
		close(putURLsCh)
	}()
	for {
		select {
		case <-ctx.Done():
			showLastProgressBar(pg, nil)
			return
		case putURLs, ok := <-putURLsCh:
			if !ok {
				showLastProgressBar(pg, nil)
				return
			}
			if putURLs.Error != nil {
				printPutURLsError(&putURLs)
				showLastProgressBar(pg, putURLs.Error.ToGoError())
				return
			}
			urls := doCopy(ctx, doCopyOpts{
				cpURLs:           putURLs,
				pg:               pg,
				encKeyDB:         encKeyDB,
				multipartSize:    size,
				multipartThreads: strconv.Itoa(threads),
			})
			if urls.Error != nil {
				e = urls.Error.ToGoError()
				showLastProgressBar(pg, e)
				return
			}
		}
	}
}

func printPutURLsError(putURLs *URLs) {
	// Print in new line and adjust to top so that we
	// don't print over the ongoing scan bar
	if !globalQuiet && !globalJSON {
		console.Eraseline()
	}
	if strings.Contains(putURLs.Error.ToGoError().Error(),
		" is a folder.") {
		errorIf(putURLs.Error.Trace(),
			"Folder cannot be copied. Please use `...` suffix.")
	} else {
		errorIf(putURLs.Error.Trace(),
			"Unable to upload.")
	}
}

func showLastProgressBar(pg ProgressReader, e error) {
	if e != nil {
		// We only erase a line if we are displaying a progress bar
		if !globalQuiet && !globalJSON {
			console.Eraseline()
		}
		return
	}
	if progressReader, ok := pg.(*progressBar); ok {
		progressReader.Finish()
	} else {
		if accntReader, ok := pg.(*accounter); ok {
			printMsg(accntReader.Stat())
		}
	}
}
