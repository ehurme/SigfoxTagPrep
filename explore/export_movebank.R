source("R/process_bat_capturesheet_to_movebank.R")
source("R/wildcloud_nanofox_to_movebank.R")

mb_ref <- process_bat_capturesheet_to_movebank(
  capture_csv = "../../../Dropbox/MPI/Noctule/Data/movebank/Belgium/capture_beligum.csv",
  out_csv     = "../../../Dropbox/MPI/Noctule/Data/movebank/Belgium/belgium-reference-data.csv",
  tz          = "CET",
  location_name  = "belgium",
  deploy_time_source = "tag_deployment_time",
  tag_count_order = "deploy_time_then_tag"
)
wildcloud_nanofox_to_movebank(
  wc_path           = "../../../Dropbox/MPI/Noctule/Data/movebank/Belgium/wildcloud/",
  movebank_csv_path = NULL,
  animals_path      = "../../../Dropbox/MPI/Noctule/Data/movebank/Belgium/belgium-reference-data.csv",
  output_dir        = "../../../Dropbox/MPI/Noctule/Data/movebank/Belgium/movebank/",
  movebank_project_name = "ICARUS Bats. Nyctalus leisleri Nyctalus noctula. Flanders",
  clean_lat_range   = NULL,
  clean_lon_range   = NULL
)

mb_ref <- process_bat_capturesheet_to_movebank(
  capture_csv = "../../../Dropbox/MPI/Noctule/Data/movebank/Catalonia and Aragon/Capture_CataloniaAragon_2023-2025.csv",
  out_csv     = "../../../Dropbox/MPI/Noctule/Data/movebank/Catalonia and Aragon/catalonia_aragon-reference-data.csv",
  tz          = "CET",
  location_name  = "catalonia_aragon",
  deploy_time_source = "tag_deployment_time",
  tag_count_order = "deploy_time_then_tag"
)
wildcloud_nanofox_to_movebank(
  wc_path           = "../../../Dropbox/MPI/Noctule/Data/movebank/Catalonia and Aragon/wildcloud/",
  movebank_csv_path = NULL,
  animals_path      = "../../../Dropbox/MPI/Noctule/Data/movebank/Catalonia and Aragon/catalonia_aragon-reference-data.csv",
  output_dir        = "../../../Dropbox/MPI/Noctule/Data/movebank/Catalonia and Aragon/movebank/",
  movebank_project_name = "ICARUS Bats. Nyctalus leisleri Nyctalus noctula Nyctalus lasiopterus. Catalonia Aragon, Spain",
  clean_lat_range   = NULL,
  clean_lon_range   = NULL
)

mb_ref <- process_bat_capturesheet_to_movebank(
  capture_csv = "../../../Dropbox/MPI/Noctule/Data/movebank/Czechia/Czechia_captures2024-2025.csv",
  out_csv     = "../../../Dropbox/MPI/Noctule/Data/movebank/Czechia/czechia-reference-data.csv",
  tz          = "CET",
  location_name  = "czechia",
  deploy_time_source = "tag_deployment_time",
  tag_count_order = "deploy_time_then_tag"
)
wildcloud_nanofox_to_movebank(
  wc_path           = "../../../Dropbox/MPI/Noctule/Data/movebank/Czechia/wildcloud/",
  movebank_csv_path = NULL,
  animals_path      = "../../../Dropbox/MPI/Noctule/Data/movebank/Czechia/czechia-reference-data.csv",
  output_dir        = "../../../Dropbox/MPI/Noctule/Data/movebank/Czechia//movebank/",
  movebank_project_name = "ICARUS Bats. Nyctalus leisleri Nyctalus noctula. Czechia",
  clean_lat_range   = NULL, #c(30, 60),
  clean_lon_range   = NULL
)

mb_ref <- process_bat_capturesheet_to_movebank(
  capture_csv = "../../../Dropbox/MPI/Noctule/Data/movebank/Denmark/captures_Denmark.csv",
  out_csv     = "../../../Dropbox/MPI/Noctule/Data/movebank/Denmark/denmark-reference-data.csv",
  tz          = "CET",
  location_name  = "denmark",
  deploy_time_source = "tag_deployment_time",
  tag_count_order = "deploy_time_then_tag"
)
wildcloud_nanofox_to_movebank(
  wc_path           = "../../../Dropbox/MPI/Noctule/Data/movebank/Denmark/wildcloud/",
  movebank_csv_path = NULL,
  animals_path      = "../../../Dropbox/MPI/Noctule/Data/movebank/Denmark/denmark-reference-data.csv",
  output_dir        = "../../../Dropbox/MPI/Noctule/Data/movebank/Denmark/movebank/",
  movebank_project_name = "ICARUS Bats. Nyctalus noctula. Denmark",
  clean_lat_range   = NULL,
  clean_lon_range   = NULL
)

# France
mb_ref <- process_bat_capturesheet_to_movebank(
  capture_csv = "../../../Dropbox/MPI/Noctule/Data/movebank/Occitanie/France_captures20242025.csv",
  out_csv     = "../../../Dropbox/MPI/Noctule/Data/movebank/Occitanie/occitanie-reference-data.csv",
  tz          = "CET",
  location_name  = "occitanie",
  deploy_time_source = "tag_deployment_time",
  tag_count_order = "deploy_time_then_tag"
)
wildcloud_nanofox_to_movebank(
  wc_path           = "../../../Dropbox/MPI/Noctule/Data/movebank/Occitanie/wildcloud/",
  movebank_csv_path = NULL,
  animals_path      = "../../../Dropbox/MPI/Noctule/Data/movebank/Occitanie/occitanie-reference-data.csv",
  output_dir        = "../../../Dropbox/MPI/Noctule/Data/movebank/Occitanie/movebank/",
  movebank_project_name = "ICARUS Bats. Nyctalus noctula Nyctalus lasiopterus. Occitanie",
  clean_lat_range   = NULL,
  clean_lon_range   = NULL
)

# Galicia
mb_ref <- process_bat_capturesheet_to_movebank(
  capture_csv = "../../../Dropbox/MPI/Noctule/Data/movebank/Galicia/captures_galicia2025.csv",
  out_csv     = "../../../Dropbox/MPI/Noctule/Data/movebank/Galicia/galicia-reference-data.csv",
  tz          = "CET",
  location_name  = "galicia",
  deploy_time_source = "tag_deployment_time",
  tag_count_order = "deploy_time_then_tag"
)
wildcloud_nanofox_to_movebank(
  wc_path           = "../../../Dropbox/MPI/Noctule/Data/movebank/Galicia/wildcloud/",
  movebank_csv_path = NULL,
  animals_path      = "../../../Dropbox/MPI/Noctule/Data/movebank/Galicia/galicia-reference-data.csv",
  output_dir        = "../../../Dropbox/MPI/Noctule/Data/movebank/Galicia/movebank/",
  movebank_project_name = "ICARUS Bats. Nyctalus lasiopterus. Galicia, Spain",
  clean_lat_range   = NULL,
  clean_lon_range   = NULL
)

#
mb_ref <- process_bat_capturesheet_to_movebank(
  capture_csv = "../../../Dropbox/MPI/Noctule/Data/movebank/Italy/captures_italy.csv",
  out_csv     = "../../../Dropbox/MPI/Noctule/Data/movebank/Italy/italy-reference-data.csv",
  tz          = "CET",
  location_name  = "italy",
  deploy_time_source = "tag_deployment_time",
  tag_count_order = "deploy_time_then_tag"
)
# wildcloud_nanofox_to_movebank(
#   wc_path           = "../../../Dropbox/MPI/Noctule/Data/movebank/Italy/wildcloud/",
#   movebank_csv_path = NULL,
#   animals_path      = "../../../Dropbox/MPI/Noctule/Data/movebank/Italy/italy-reference-data.csv",
#   output_dir        = "../../../Dropbox/MPI/Noctule/Data/movebank/Italy/movebank/",
#   movebank_project_name = "ICARUS Bats. Nyctalus leisleri. Italy",
#   clean_lat_range   = NULL,
#   clean_lon_range   = NULL
# )

mb_ref <- process_bat_capturesheet_to_movebank(
  capture_csv = "../../../Dropbox/MPI/Noctule/Data/movebank/Navarre/navarre_capturesheet2023-2025.csv",
  out_csv     = "../../../Dropbox/MPI/Noctule/Data/movebank/Navarre/navarre-reference-data.csv",
  tz          = "CET",
  location_name  = "navarre",
  deploy_time_source = "tag_deployment_time",
  tag_count_order = "deploy_time_then_tag"
)

wildcloud_nanofox_to_movebank(
  wc_path           = "../../../Dropbox/MPI/Noctule/Data/movebank/Navarre/wildcloud/",
  movebank_csv_path = NULL,
  animals_path      = "../../../Dropbox/MPI/Noctule/Data/movebank/Navarre/navarre-reference-data.csv",
  output_dir        = "../../../Dropbox/MPI/Noctule/Data/movebank/Navarre/movebank/",
  movebank_project_name = "ICARUS Bats. Nyctalus leisleri Nyctalus noctula Nyctalus lasiopterus. Navarre, Spain",
  clean_lat_range   = NULL,
  clean_lon_range   = NULL
)

mb_ref <- process_bat_capturesheet_to_movebank(
  capture_csv = "../../../Dropbox/MPI/Noctule/Data/movebank/Poland/poland_captures.csv",
  out_csv     = "../../../Dropbox/MPI/Noctule/Data/movebank/Poland/poland-reference-data.csv",
  tz          = "CET",
  location_name  = "poland",
  deploy_time_source = "tag_deployment_time",
  tag_count_order = "deploy_time_then_tag"
)
wildcloud_nanofox_to_movebank(
  wc_path           = "../../../Dropbox/MPI/Noctule/Data/movebank/Poland/wildcloud/",
  movebank_csv_path = NULL,
  animals_path      = "../../../Dropbox/MPI/Noctule/Data/movebank/Poland/poland-reference-data.csv",
  output_dir        = "../../../Dropbox/MPI/Noctule/Data/movebank/Poland/movebank/",
  movebank_project_name = "ICARUS Bats. Nyctalus noctula. Poland",
  clean_lat_range   = NULL,
  clean_lon_range   = NULL
)

mb_ref <- process_bat_capturesheet_to_movebank(
  capture_csv = "../../../Dropbox/MPI/Noctule/Data/movebank/Sistema Central/captures_sistemacentral2025.csv",
  out_csv     = "../../../Dropbox/MPI/Noctule/Data/movebank/Sistema Central/sistemacentral-reference-data.csv",
  tz          = "CET",
  location_name  = "sistemacentral",
  deploy_time_source = "tag_deployment_time",
  tag_count_order = "deploy_time_then_tag"
)
wildcloud_nanofox_to_movebank(
  wc_path           = "../../../Dropbox/MPI/Noctule/Data/movebank/Sistema Central/wildcloud/",
  movebank_csv_path = NULL,
  animals_path      = "../../../Dropbox/MPI/Noctule/Data/movebank/Sistema Central/sistemacentral-reference-data.csv",
  output_dir        = "../../../Dropbox/MPI/Noctule/Data/movebank/Sistema Central/movebank/",
  movebank_project_name = "ICARUS Bats. Nyctalus leisleri Nyctalus lasiopterus. Sistema Central, Spain",
  clean_lat_range   = NULL,
  clean_lon_range   = NULL
)

mb_ref <- process_bat_capturesheet_to_movebank(
  capture_csv = "../../../Dropbox/MPI/Noctule/Data/movebank/Switzerland/swiss_captures.csv",
  out_csv     = "../../../Dropbox/MPI/Noctule/Data/movebank/Switzerland/swiss-reference-data.csv",
  tz          = "CET",
  location_name  = "swiss",
  deploy_time_source = "tag_deployment_time",
  tag_count_order = "deploy_time_then_tag"
)
wildcloud_nanofox_to_movebank(
  wc_path           = "../../../Dropbox/MPI/Noctule/Data/movebank/Switzerland/wildcloud/",
  movebank_csv_path = NULL,
  animals_path      = "../../../Dropbox/MPI/Noctule/Data/movebank/Switzerland/swiss-reference-data.csv",
  output_dir        = "../../../Dropbox/MPI/Noctule/Data/movebank/Switzerland//movebank/",
  movebank_project_name = "ICARUS Bats. Nyctalus leisleri Nyctalus noctula. Switzerland",
  clean_lat_range   = NULL,
  clean_lon_range   = NULL
)

