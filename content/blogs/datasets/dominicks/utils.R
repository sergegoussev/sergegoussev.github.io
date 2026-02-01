library(tidyverse)
library(jsonlite)
library(arrow)
library(glue)


#' Validate whether actions are required depending on the category name
#'
#' @param category_name The name of the category (e.g., "Bottled Juices").
#' @param url_json_path Path to the urls.json file that stores the
#' URLs to download specific files
#' @param data_dir The directory to save the raw data.
#' Expects: /data/raw for raw .csv, /data-pre-processed for .parquet
#' versions of the data and for
#'
#' @return TRUE/FALSE whether downloading and processing the category is needed
is_downloading_required <- function(category_name, url_json_path="urls.json") {   
 urls <- fromJSON(url_json_path)
 if category_name not in urls["category_files"] {
        stop("No such category")
    }
    # TO DO... finish
    # if (file.exists(full_path) && grepl("\\.json$", file_name)) {
    #     return(TRUE)
    # } else {
    #     return(FALSE)
    # }
}


#' Small function to convert .csv to .parquet
#' 
#' @param file_name name of the file
#' Note: assumes that /data/raw/ is where .csv files are in
#' and saves files to /data/semi-processed/
#' @param cols_to_remove specify list of columns to drop if not needed
#' eg: cols_to_remove <- c("address", "phone_number", "internal_notes")
#' Note: this is valuable as some extra columns may not be needed
convert_csv_to_parquet <- function(
    file_name,
    cols_to_remove = c("PRICE_HEX", "PROFIT_HEX") #default
    ) {
    # 1. Read CSV into an R data frame
    df <- read.csv(glue("../../../../data/raw/{file_name}.csv"))

    # 2. Clean the dataframe
    df_clean <- df %>% select(-all_of(cols_to_remove))

    # 3. Write that data frame to Parquet
    write_parquet(df_clean, glue("../../../../data/semi-processed/{file_name}.parquet"))
}

# convert_csv_to_parquet('wbjc')
# convert_csv_to_parquet('wcoo')
# convert_csv_to_parquet('upcbjc', cols_to_remove=c())
# convert_csv_to_parquet('upccoo', cols_to_remove=c())


move <- read_parquet(
    "../../../../data/semi-processed/wbjc.parquet"
) %>%
  filter(
    OK == 1 & PRICE > 0
  ) %>%
  mutate(
    SALES = PRICE * MOVE / QTY
  ) %>%
  select(WEEK, STORE, UPC, MOVE, SALES, SALE,# PROFIT
  )

# head(move)
# str(move)

upc <- read_parquet(
    "../../../../data/semi-processed/upcbjc.parquet"
) %>%
  select(COM_CODE, NITEM, UPC,# DESCRIP, # SIZE
  )

# str(upc)