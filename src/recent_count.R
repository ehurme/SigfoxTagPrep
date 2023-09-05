# how many tags sent messages in the past 2 days?
# data <- belgium
# days <- 2
recent_count <- function(data, days){
  idx <- which(difftime(Sys.time(), data$datetime, units = "days") < days)
  recent <- data[idx,]
  t <- table(recent$Device)
  print(paste0(length(t), " tags have sent messages in the past ", days, " days"))
}