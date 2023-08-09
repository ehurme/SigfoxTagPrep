# download and plot summer 23 data
source("./R/sigfox_download.R")

## deployments must be a data frame with ID, deployment day,
deployments = readxl::read_xlsx("//10.0.16.7/grpDechmann/Bat projects/Noctule captures/2023/July 2023 TinyFoxBatt deployments.xlsx", sheet = 1)
# View(deployments)
deployments = deployments[deployments$`tag ID` != "0120D1F8",]

summer23 <- sigfox_download(ID = deployments$`PIT-tag`,
        ring = deployments$`ring #`,
        tag_ID = deployments$`tag ID`,
        attachment_type = deployments$`attachment method`,
        capture_weight = deployments$`mass of bat`,
        capture_time = strptime(paste0(deployments$`attachment date`, " ",
                                       str_sub(deployments$`attachment time`,-8,-1)),
                                format = "%d.%m.%y %H:%M:%S"),
        FA_length = deployments$`FA length`, # length in mm
        tag_weight = deployments$`tag mass`, # weight in grams
        sex = deployments$sex,
        age = deployments$age,
        repro_status = deployments$`repro status`,
        species = paste0(deployments$genus, " ", deployments$species),
        release_time = strptime(paste0(deployments$`attachment date`, " ",
                                       str_sub(deployments$`attachment time`,-8,-1)),
                                format = "%d.%m.%y %H:%M:%S"),
        latitude = sapply(strsplit(deployments$location, ","), "[", 1) %>% as.numeric(),
        longitude = sapply(strsplit(deployments$location, ","), "[", 2) %>% as.numeric(),
        roost = deployments$roost,
        plot_map = TRUE,
        buffer = 1,
        plot_vedba = TRUE,
        facet_location = TRUE,
        save_maps = TRUE,
        save_path = "../../../Dropbox/MPI/Noctule/Plots/Summer23/")

