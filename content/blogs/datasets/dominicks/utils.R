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


weeks <- read_csv(
  '../../../../data/raw/weeks.csv',
  col_names = c('WEEK','START','END','SPECIAL_EVENTS')
  ) %>%
  mutate(
    START = as.Date(START,format = '%m/%d/%y'), #convert to date format
    END = as.Date(END,format = '%m/%d/%y'),     #convert to date format
    REF_PERIOD = paste(format(END, '%Y'),format(END, '%m'),sep = '-'), #reference period as string
    WEEK_FULLY_IN_MONTH = ifelse(months(START) == months(END),TRUE,NA),#return NA if the week straddles months
  ) %>% #create a count of the week that is cleanly within the month
  group_by(REF_PERIOD) %>%
  mutate(
    WEEK_OF_MONTH = ifelse(
      !is.na(WEEK_FULLY_IN_MONTH),
      cumsum(!is.na(WEEK_FULLY_IN_MONTH)),
      NA_integer_
    )
  ) %>% 
  ungroup() %>%
  select(WEEK,REF_PERIOD,WEEK_OF_MONTH,START,END)# SPECIAL_EVENTS)

write_parquet(weeks, "../../../../data/semi-processed/weeks.parquet")


# convert_csv_to_parquet('wbjc')
# convert_csv_to_parquet('wcoo')
# convert_csv_to_parquet('upcbjc', cols_to_remove=c())
# convert_csv_to_parquet('upccoo', cols_to_remove=c())



#' Function to process various files to create a master raw data file
#' 
#' @param category_name name of the category (short form)
#' @param relative_dir where relative to this script are the data
#' NOTE: assumes /data/semi-processed/ and /data/raw/ as the source
#' 
#' @return master transaction dataframe
pre-process_category <- function(category_name) {
    #1. read the movement data (transactions)
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

    #2. read the UPC (product) data
    upc <- read_parquet(
        "../../../../data/semi-processed/upcbjc.parquet"
    ) %>%
    select(COM_CODE, NITEM, UPC,# DESCRIP, # SIZE
    )

    #3. Read week definitions
    weeks <- read_parquet(
        "../../../../data/semi-processed/weeks.parquet"
    )


    #4. Read store data
    stores <- read_csv(
    '../../../../data/raw/stores.csv',
    col_names = c('STORE','CITY','PRICE_TIER','ZONE','ZIP_CODE','ADDRESS')
    ) %>%
    select(STORE,PRICE_TIER,ZONE,
        # CITY,
        # ZIP_CODE,
        # ADDRESS
    )

    #5. Merge all files
    move <- move %>%
    left_join(upc,by = 'UPC'
    ) %>%
    left_join(weeks,by = 'WEEK'
    ) %>%
    left_join(stores,by = 'STORE'
    )

    #6. Clean file
    move <- move %>%
    mutate(
        SALE = if_else(!is.na(SALE),1,0),
        COM_CODE = if_else(!is.na(COM_CODE),COM_CODE,999),
        NITEM = if_else(!is.na(NITEM) & NITEM >= 0,NITEM,UPC),
        PRICE_TIER = if_else(!is.na(PRICE_TIER),PRICE_TIER,'NA'),
        ZONE = if_else(!is.na(ZONE),ZONE,0)
    )

    #7. Return the file
    return(move)
}
