#' Title: Script of utility functions to simplify getting and processing Dominick's data
#' Description: Script has several functions that can be called to do various things.
#' Author: Serge Goussev
#' References: Most functions inspired by code shared by Eurostat (Jens Mehrhoff et al),
#' see: https://github.com/eurostat/dff/blob/master/R/dff_tidyverse.R
# -------------------------------------------------------------------------

library(tidyverse)
library(jsonlite)
library(arrow)
library(glue)

setwd(dirname(rstudioapi::getActiveDocumentContext()$path))

#-----------------WIP-------------------


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
# is_downloading_required <- function(category_name, url_json_path="urls.json") {   
#  urls <- fromJSON(url_json_path)
#  if category_name not in urls["category_files"] {
#         stop("No such category")
#     }
#     # TO DO... finish
#     # if (file.exists(full_path) && grepl("\\.json$", file_name)) {
#     #     return(TRUE)
#     # } else {
#     #     return(FALSE)
#     # }
# }


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

# -----------------CLEAN-----------------------------------------

#' Function to do once time processing of week and store
#' data from its raw csv to ready to use parquet. Note these 
#' files are downloaded from the web directly
#' 
#' @param save_dir directory to save the files to
download_and_process_weeks_and_stores_data <- function(save_dir) {
    #1. Process week file
    weeks <- read_csv(
    'https://raw.githubusercontent.com/eurostat/dff/master/CSV/weeks.csv',
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
    message("weeks file downloaded and processed")
    print(head(weeks))

    print(save_dir)
    #Save the weekly file
    write_parquet(weeks, glue("{save_dir}/weeks.parquet"))
    message(glue("weeks file saved into {save_dir}"))

    stores <- read_csv(
        'https://raw.githubusercontent.com/eurostat/dff/master/CSV/stores.csv',
        col_names = c('STORE','CITY','PRICE_TIER','ZONE','ZIP_CODE','ADDRESS')
        ) %>%
        select(STORE,PRICE_TIER,ZONE,
            # CITY,
            # ZIP_CODE,
            # ADDRESS
        )
    message("stores file downloaded and processed")

    #save the stores file
    write_parquet(stores, glue("{save_dir}/stores.parquet"))
    message(glue("stores file saved in: {save_dir}"))
}

# download_and_process_weeks_and_stores_data('../../../../data/semi-processed')


#' Function to process the category and the supporting dataframes
#' (i.e. weeks and stores) to create a master raw data file for all 
#' transactions in the category.
#' 
#' Note: this function creates a holistic dataset of all transactions, products,
#' stores, and weekly definitions *before* pre-price index aggregation, i.e
#' before you do the praggreagation across time, geography, and products.
#' 
#' @param category_name name of the category (short form)
#' @param data_dir where the data files to process live data
#' @param save TRUE/FALSE binary to specify whether to save or not
#' @param output_dir where to save the preprocessed data
#' 
#' @return master transaction dataframe
preprocess_category_data <- function(category_name, data_dir, save=FALSE, output_dir) {
    #1. read the movement data (transactions)
    move <- read_parquet(
        glue("{data_dir}/w{category_name}.parquet")
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
        glue("{data_dir}/upc{category_name}.parquet")
    ) %>%
    select(COM_CODE, NITEM, UPC, DESCRIP, # SIZE
    )

    #3. Read week definitions
    weeks <- read_parquet(
        glue("{data_dir}/weeks.parquet")
    )

    #4. Read store data
    stores <- read_parquet(
    glue('{data_dir}/stores.parquet'),
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

    # #6. Clean file
    move <- move %>%
    mutate(
        SALE = if_else(!is.na(SALE),1,0),
        COM_CODE = if_else(!is.na(COM_CODE),COM_CODE,999),
        NITEM = if_else(!is.na(NITEM) & NITEM >= 0,NITEM,UPC),
        PRICE_TIER = if_else(!is.na(PRICE_TIER),PRICE_TIER,'NA'),
        ZONE = if_else(!is.na(ZONE),ZONE,0)
    )

    #7. Save and return the file
    if (save) {
        write_parquet(move, glue("{output_dir}/processed_{category_name}.parquet"))
    }
    return(move)

}

# move = preprocess_category_data('bjc','../../../../data/semi-processed', TRUE, '../../../../data/semi-processed')

#' Function to do aggregation across time, outlets, and item codes. This first
#' and critical step in any multilateral methods is key to define the 
#' homogeneous prodcuts. The output of this step thus serves as the input for 
#' elementary price index aggregation.
#' 
#' @param category_name name of the category of interest
#' @param data_dir directory where the preprocessed cateogry data resides
#' 
#' @param time_sample that will be used within the month (e.g. c(1,2) will 
#' mean that weeks 1 and 2 will be used from each month's data)
#' @param group_by_parameters list the categories that are used to differentiate
#' homogenous products, e.g. (COM_CODE, NITEM, STORE) will aggregate across
#' NITEM code in each category, i.e. STOREs will be ignored.
#' @param window dictionary that specifies the start and end of the months 
#' that are extracted from the input data window['start'] = '1990-01-01' and 
#' window['end'] = '1992-01-01' will pull 25 months of data
#'  
#' @param save TRUE/FALSE whether to save the output dataframe
#' @param output_dir where to save the output dataframe
#' 
#' @output homogeneous product dataframe
homogenous_product_aggregation <- function(
    category_name,
    data_dir,
    time_sample,
    group_by_parameters,
    window,
    save = FALSE,
    output_dir = NA
) {
    move = read_parquet(glue("{data_dir}/processed_{category_name}.parquet"))
    message("data read into memory")
    move_monthly <- move %>%
    filter(
        between(
        START,
        as.Date(window$start),
        as.Date(window$end)
        )
    ) %>%
    filter(
        WEEK_OF_MONTH %in% time_sample # Filter for specific weeks within the month
    ) %>%
    group_by(across(all_of(group_by_parameters))
    ) %>%
    summarise(
        MOVE = sum(MOVE),
        SALES = sum(SALES)
    )
    message("aggregated")
    print(head(move_monthly))

    move_monthly <- move_monthly %>%
    group_by(
        REF_PERIOD
    ) %>%
    mutate(
        PRICE = SALES / MOVE,
        SHARE = SALES / sum(SALES)
    )
    message("unit prices and sale proporitions calculated")
    #TODO: drop unecessary columns, return and save data frame
    if (save) {
        write_parquet(move_monthly, glue("{output_dir}/ird_{category_name}.parquet"))
    }
    return(move_monthly)
}


#--------------------------

if (sys.nframe() == 0) {
    #Run only if the script is run directly

    ird <- homogenous_product_aggregation(
        category_name='bjc',
        data_dir='../../../../data/semi-processed',
        time_sample=c(1,2),
        group_by_parameters=c('NITEM', 'REF_PERIOD'),
        window=list(
        "start" = "1990-01-01",
        "end"   = "1990-03-01")
    )
    head(ird)
    # filtered_ird <- ird %>% 
    #     filter(REF_PERIOD == "1990-01")
    # filtered_ird
    # write_csv(filtered_ird, "1990-01-ird.csv")


}

tg <- with(ird, tornqvist_geks(PRICE, SHARE, REF_PERIOD, NITEM, window=3))
tg

library("gpindex")

df <- data.frame(
  price    = 1:10,
  quantity = 10:1,
  period   = rep(1:5, 2),
  product  = rep(letters[1:2], each = 5)
)
str(df)

tg_results <- with(df, tornqvist_geks(price, quantity, period, product, window = 3))
splice_index(tg_results)