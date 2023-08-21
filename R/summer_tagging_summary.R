# summarize summer tagging

summer23$activity <- summer23$`24h Active (%)`
summer23 %>% reframe(
  start = min(datetime),
  end = max(datetime[activity > 10]),
  duration = difftime(max(datetime[activity > 10]), min(datetime),units = "days"),
  .by = c(Device, attachment_type, bat_weight, species, capture_latitude, capture_longitude) ) -> sum23
sum23

ggplot(sum23, aes(x = attachment_type, y = duration))+
  geom_violin()+
  facet_wrap(~species, scales = "free_x")+
  theme(axis.text.x = element_text(angle = 30, vjust = 0.5, hjust=0.5))


poland23$roost %>% table
poland23$activity <- poland23$`24h Active (%)`

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

library(ggpubr)
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