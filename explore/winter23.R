# winter tagging

deployments = readxl::read_xlsx("//10.0.16.7/grpDechmann/Bat projects/Noctule captures/2023/Winter 2023 captures.xlsx", sheet = 1)

deployments$`tag ID` %>% unique
deployments <- deployments[deployments$`tag ID` != "n/a",]

# download all summer deployments
winter23 <- sigfox_download(ID = deployments$`PIT-tag`,
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
                            # latitude = sapply(strsplit(deployments$location, ","), "[", 1) %>% as.numeric(),
                            # longitude = sapply(strsplit(deployments$location, ","), "[", 2) %>% as.numeric(),
                            roost = deployments$roost)

winter23$tag_ID
plot(winter23$longitude, winter23$latitude)

w23 <- sigfox_to_move2(winter23, legend = TRUE)

