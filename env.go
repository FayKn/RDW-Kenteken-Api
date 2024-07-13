package main

import (
	"github.com/joho/godotenv"
	"log"
	"os"
	"strconv"
)

func getEnvVar(varName string) string {
	err := godotenv.Load()
	if err != nil {
		log.Println("Error loading .env file")
	}

	var envVar = os.Getenv(varName)

	return envVar
}

func getIntEnvVar(varName string) int {
	err := godotenv.Load()
	if err != nil {
		log.Println("Error loading .env file")
	}

	var envVar = os.Getenv(varName)
	var intVar, _ = strconv.Atoi(envVar)

	return intVar
}
