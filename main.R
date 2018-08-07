source("header.R")

# Get test samples --------------------------------------------------------

Forecast <- conn("Forecast")
Laputa <- conn("Laputa")

# collect contract_with_landlords
contract_with_landlords <-
  tbl(Laputa, "contract_with_landlords") %>%
  select(id,
         suite_id,
         house_pricing_id,
         type,
         manage_status,
         status,
         stage,
         sign_date) %>%
  filter(!is.na(suite_id),!is.na(house_pricing_id)) %>%
  filter(status == "已签约" , stage == "执行中") %>%
  filter(sign_date > "2018-07-03", sign_date <= "2018-08-03") %>%
  collect()

# collect house_pricing
house_pricing <-
  tbl(Forecast, "house_pricing") %>%
  select(
    house_pricing_id = id,
    plan_id,
    sales_wish_decoration_cost,
    original_house_state,
    sale_product,
    sale_product_type,
    rent_model,
    puzu_price,
    sales_wish_sum_price_yuan,
    contract_period,
    free_day_of_per_year_json,
    price_for_per_year_json,
    free_room_from_system,
    price_of_per_year_from_system
  ) %>%
  collect()

# collect house_state_plan_ticket
house_state_plan_ticket <-
  tbl(Forecast, "house_state_plan_ticket") %>%
  select(
    plan_id = id,
    house_resource_id,
    sale_room_status,
    building_area = area,
    before_room_num,
    before_public_bathroom_num,
    before_private_bathroom_num,
    after_room_num,
    after_public_bathroom_num,
    after_private_bathroom_num
  ) %>%
  collect()

# collect house_resources
house_resources <- tbl(Laputa, 'house_resources') %>%
  select(id, xiaoqu_id) %>%
  collect()

# collect xiaoqus
xiaoqus <- tbl(Laputa, 'xiaoqus') %>%
  select(id, city, name, longitude, latitude) %>%
  collect()


test_samples <- contract_with_landlords %>%
  left_join(house_pricing, by = "house_pricing_id") %>%
  left_join(house_state_plan_ticket, by = "plan_id") %>%
  left_join(house_resources, by = c("house_resource_id" = "id")) %>%
  left_join(xiaoqus, by = c("xiaoqu_id" = "id")) %>%
  filter_at(
    vars(
      "house_pricing_id",
      "plan_id",
      "house_resource_id",
      "xiaoqu_id"
    ),
    all_vars(!is.na(.))
  )


# Test --------------------------------------------------------------------

fac_api_config <- read_api_config('fengkong_accurate_evaluation')
endpoint <- fac_api_config$endpoint
token <- fac_api_config$token

set.seed(1234)
sub_test_samples <- test_samples[sample(x = 1:nrow(test_samples), 10), ]

test_fun <- function(x, token, endpoint, sub_test_samples) {
  case <- sub_test_samples[x,] %>% select(
    plan_id,
    business_model = sale_product,
    house_model = sale_product_type,
    rent_model,
    house_cost = sales_wish_decoration_cost,
    model_room_status = original_house_state
  ) %>%
    mutate(token = token)
  res <- call_api_safely(case, endpoint)
  if(length(res$error)==0){
    res$error <- ""
  }else{
    res$error <- as.character(res$error)
  }
  res
}

test_res <- map(1:10, test_fun, token, endpoint, sub_test_samples) %>% bind_rows()

cat_rule("Ready to export result", col = "lightblue")

test_sam_res <- bind_cols(sub_test_samples, test_res)

write_into_db(test_sam_res)

cat_rule("Exported result.", col = "lightblue")

# trigger validator
read_api_config("Validator") %>% GET()

cat_rule("Mission complete.", col = "green")