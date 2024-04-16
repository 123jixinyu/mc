package cmd

import "strings"

func getPrefix() string {

	auth := getAuth()

	return AuthAlias + "/" + auth.Bucket + auth.BasePath
}

func getUrlWithSeparator(v string) string {

	if !strings.HasPrefix(v, "/") {
		return "/" + v
	}
	return v
}

func getFullPath(path string) string {

	if path == "." {
		path = "/"
	}
	return getPrefix() + getUrlWithSeparator(path)
}
