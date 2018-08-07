# Load packages -----------------------------------------------------------
library(tidyverse)
library(dbplyr)
library(jsonlite)
library(mongolite)
library(DBI)
library(yaml)
library(httr)
library(cli)
# Function to connect mysql -----------------------------------------------
conn <- function(conn, drv = RMySQL::MySQL()) {
  
  # if file not found, stop
  if (!file.exists("dbconfig.yaml")) {
    stop("Database credential not found.")
  }
  
  # read config
  d <- yaml::read_yaml("dbconfig.yaml")
  
  # check if db is specified
  if( "db" %in% names(d[[conn]]) ){
    db = d[[conn]][['db']]
  }else{
    db = conn
  }
  
  # check if the credentials are specified
  if(!all(c("user", "password", "host", "port") %in% names(d[[conn]]))){
    stop("One or more database parameters is incorrect.")
  }
  
  # est conn
  c <- DBI::dbConnect(
    drv,
    user = d[[conn]][["user"]],
    password = d[[conn]][["password"]],
    host = d[[conn]][["host"]],
    port = as.numeric(d[[conn]][["port"]]),
    db = db
  )
  
  # return connection
  c
  
}



# Function to read apiconfig ----------------------------------------------
read_api_config <- function(which_api) {
  # make sure file exists
  if(!file.exists("apiconfig.yaml")){
    stop("Require config file - R/apiconfig.yaml")
  }
  
  # read config
  d <- read_yaml("apiconfig.yaml")
  
  # check if the credentials are specified
  if(!all(c("host", "port", "swagger") %in% names(d[[which_api]]))){
    stop(paste("One or more parameter in", which_api, "is missing."))
  }
  
  # return a list of token and endpoint
  list(
    token = d[[which_api]][["token"]],
    endpoint = with(d[[which_api]], paste0(host, ":", port, "/", swagger))
  )
}


# Function to call api ----------------------------------------------------
call_api_defensive <- function(case, endpoint) {
  # timeout set to 5 secs is more than enough
  resp = POST(endpoint, body = case, encode = "form", timeout(15))
  
  # only accept JSON object
  if (http_type(resp) != "application/json") {
    return(NULL)
  }
  
  # only accept 200 (success)
  if (status_code(resp) != 200) {
    return(NULL)
  }
  
  # parse response
  content(resp)
}
call_api_safely <- purrr::safely(call_api_defensive)

# Function to connect mongo -----------------------------------------------
est_mongo_conn <- function(conn) {
  
  # make sure file exists
  if (!file.exists("dbconfig.yaml")) {
    stop("Database credential not found.")
  }
  
  # read config
  d <- yaml::read_yaml("dbconfig.yaml")
  
  # check if db is specified
  if( "db" %in% names(d[[conn]]) ){
    db = d[[conn]][['db']]
  }else{
    db = conn
  }
  
  # check if the credentials are specified
  if(!all(c("host", "port", "collection") %in% names(d[[conn]]))){
    stop("One or more database parameters is incorrect.")
  }
  
  # est conn
  c <- mongo(
    collection = d[[conn]][["collection"]],
    url = with(d[[conn]], 
               # mongodb://username:password@host:port
               sprintf("mongodb://%s:%s@%s:%d/", user, password, host, port)),
    db = db
  )
  
  # return connection
  c
  
}
# write_into_db -----------------------------------------------------------

write_into_db <- function(df){

  mg_write <- est_mongo_conn("Mongo")

  mg_write$insert(df)

  rm(mg_write)

}
