# summarize summer tagging
library(pacman)
p_load(ggplot2, dplyr, data.table, magrittr, tidyverse, lubridate, ggpubr)

summer23$activity <- summer23$`24h Active (%)`
summer23 %>% reframe(
  start = min(datetime),
  end = max(datetime[activity > 10]),
  duration = difftime(max(datetime[activity > 10]), min(datetime),units = "days"),
  .by = c(Device, attachment_type, bat_weight, species, capture_latitude, capture_longitude) ) -> sum23
sum23

table(sum23$attachment_type)
ggplot(sum23[sum23$attachment_type != "collar sew and drop of sauer glue on back of back",],
       aes(x = attachment_type, y = duration))+
  geom_violin()+
  geom_jitter(width = 0.3)+
  facet_wrap(~species, scales = "free_x")+
  theme(axis.text.x = element_text(angle = 30, vjust = 1, hjust=0.5))


load("./../../../Dropbox/MPI/Noctule/Data/rdata/Sigfox_noctule_migration22.robj")
n22s$Device <- n22s@trackId
n22s$attachment_type <- "collar"
n22s$bat_weight <- NA
n22s$species <- "Nyctalus noctula"

n22 <- as.data.frame(n22s)
n22 %>% reframe(
  start = min(timestamp),
  end = max(timestamp[gps_dop > 10]),
  duration = difftime(max(timestamp[gps_dop > 10]), min(timestamp),units = "days"),
  .by = c(Device, attachment_type, bat_weight, species) ) -> spr22
spr22
spr22$capture_latitude <- 47.5718
spr22$capture_longitude <- 8.8942



spring23 <- fread("../../../Dropbox/MPI/Noctule/Data/TinyFoxBattNoctules/Common_Noctule_migration_Spring23.csv")
spring23

spring23$activity <- spring23$`24h Active (%)`
spring23$attachment_type %>% table()


spring23 %>% reframe(
  start = min(datetime),
  end = max(datetime[activity > 10]),
  duration = difftime(max(datetime[activity > 10]), min(datetime),units = "days"),
  .by = c(Device, attachment_type, bat_weight, species, capture_latitude, capture_longitude) ) -> spr23

sum23$season <- "summer"
spr23$season <- "spring"
spr22$season <- "spring"

ss23 <- rbind(sum23, spr22, spr23)
ss23$attach <- NA
ss23$attach[grepl(pattern = "sauer", ss23$attachment_type)] <- "sauer"
ss23$attach[grepl(pattern = "torbot bonding", ss23$attachment_type)] <- "torbot"
ss23$attach[grepl(pattern = "collar", ss23$attachment_type)] <- "collar"

ss23$year <- year(ss23$start)

ggplot(ss23[ss23$attachment_type != "collar sew and drop of sauer glue on back of back",],
       aes(x = attach, y = duration, col = season))+
  geom_violin()+
  geom_jitter(width = 0.3)+
  facet_wrap(~species+year, scales = "free_x")+
  theme(axis.text.x = element_text(angle = 30, vjust = 1, hjust=0.5))















# Poland
poland23 <- summer23[summer23$latitude > 50,]

poland23 %>% reframe(
  start = min(datetime),
  end = max(datetime[activity > 10]),
  duration = difftime(max(datetime[activity > 10]), min(datetime),units = "days"),
  .by = c(Device, attachment_type, bat_weight) ) -> p_sum

table(p_sum$attachment_type)
ggplot(p_sum, aes(x = attachment_type, y = duration))+
  geom_violin()+
  geom_boxplot(width = 0.2)+
  geom_jitter(width = 0.2)

wilcox.test(p_sum$duration[p_sum$attachment_type == "sauer-hautkleber"] %>% as.numeric,
            p_sum$duration[p_sum$attachment_type != "sauer-hautkleber"] %>% as.numeric)


bxp <- ggboxplot(p_sum, x = "attachment_type", y = "duration")
bxp + geom_pwc(aes(group = attachment_type),  tip.length = 0,
               method = "wilcox_test")+
  xlab("Attachment Type")+
  ylab("Tag transmission duration (days)")

# plot active foraging bats
p_act <- ggplot(poland23[poland23$activity > 0 &
                  poland23$activity < 90 &
                  date(poland23$datetime) < "2023-08-07",],
       aes(x = datetime, y = activity, col = Device))+
  geom_path()+
  geom_point()+
  ylab("24h Activity %")

# plot active foraging bats
p_temp <- ggplot(poland23[poland23$activity > 0 &
                           poland23$activity < 90 &
                           date(poland23$datetime) < "2023-08-07",],
                aes(x = datetime, y = `24h Max. Temperature (°C)`, col = Device))+
  geom_path()+
  geom_point()+
  ylab("24h Max Temp (C)")
p_temp

ggarrange(p_act, p_temp, common.legend = TRUE)

# brittany
brittany23 <- summer23[summer23$latitude > 45 & summer23$longitude < 10,]
brittany23$activity <- brittany23$`24h Active (%)`
table(brittany23$roost)

brittany23 %>% reframe(
  start = min(datetime),
  end = max(datetime[activity > 10]),
  duration = difftime(max(datetime[activity > 10]), min(datetime),units = "days"),
  .by = c(Device, attachment_type, bat_weight, roost) ) -> b_sum

table(b_sum$attachment_type)
table(b_sum$roost)

ggplot(b_sum, aes(x = attachment_type, y = duration))+
  geom_violin()+
  geom_boxplot(width = 0.2)+
  geom_jitter(width = 0.2)

wilcox.test(b_sum$duration[b_sum$attachment_type == "sauer-hautkleber"] %>% as.numeric,
            b_sum$duration[b_sum$attachment_type != "sauer-hautkleber"] %>% as.numeric)

library(ggpubr)
bxp <- ggboxplot(b_sum, x = "attachment_type", y = "duration")
bxp + geom_pwc(aes(group = attachment_type),  tip.length = 0,
               method = "wilcox_test")+
  xlab("Attachment Type")+
  ylab("Tag transmission duration (days)")

# plot active foraging bats
b_act <- ggplot(brittany23[brittany23$activity > 0 &
                           brittany23$activity < 90 ,],
                aes(x = datetime, y = activity, col = Device))+
  geom_path()+
  geom_point()+
  ylab("24h Activity %")
b_act
# plot active foraging bats
b_temp <- ggplot(brittany23[brittany23$activity > 0 &
                            brittany23$activity < 90,],
                 aes(x = datetime, y = `24h Max. Temperature (°C)`, col = Device))+
  geom_path()+
  geom_point()+
  ylab("24h Max Temp (C)")
b_temp

ggarrange(b_act, b_temp, common.legend = TRUE)


## spain
spain23 <- summer23[summer23$latitude < 40,]

spain23$activity <- spain23$`24h Active (%)`
table(spain23$roost, spain23$Device)

spain23 %>% reframe(
  start = min(datetime),
  end = max(datetime[activity > 10]),
  duration = difftime(max(datetime[activity > 10]), min(datetime),units = "days"),
  .by = c(Device, attachment_type, bat_weight, roost) ) -> s_sum

table(s_sum$attachment_type)
table(s_sum$roost)

ggplot(s_sum, aes(x = attachment_type, y = duration))+
  geom_violin()+
  geom_boxplot(width = 0.2)+
  geom_jitter(width = 0.2)

wilcox.test(s_sum$duration[s_sum$attachment_type == "sauer-hautkleber"] %>% as.numeric,
            s_sum$duration[s_sum$attachment_type != "sauer-hautkleber"] %>% as.numeric)

library(ggpubr)
bxp <- ggboxplot(s_sum, x = "attachment_type", y = "duration")
bxp + geom_pwc(aes(group = attachment_type),  tip.length = 0,
               method = "wilcox_test")+
  xlab("Attachment Type")+
  ylab("Tag transmission duration (days)")

# plot active foraging bats
b_act <- ggplot(spain23[spain23$activity > 0 &
                             spain23$activity < 90 ,],
                aes(x = datetime, y = activity, col = Device))+
  geom_path()+
  geom_point()+
  ylab("24h Activity %")
b_act
# plot active foraging bats
b_temp <- ggplot(spain23[spain23$activity > 0 &
                              spain23$activity < 90,],
                 aes(x = datetime, y = `24h Max. Temperature (°C)`, col = Device))+
  geom_path()+
  geom_point()+
  ylab("24h Max Temp (C)")
b_temp

ggarrange(b_act, b_temp, common.legend = TRUE)