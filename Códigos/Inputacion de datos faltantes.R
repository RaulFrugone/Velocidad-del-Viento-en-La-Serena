# Cargar paquetes necesarios
library(readr)
library(readxl)
library(dplyr)
library(tidyr)
library(lubridate)
library(writexl)

# ----------------------------------
# Cargar base unida
# ----------------------------------
datos <- read_xlsx("C:/Users/raul_/OneDrive - alu.ucm.cl/Universidad/Primer Semestre 2025/Series de Tiempo/Proyecto final/datos_220002.xlsx")

# ----------------------------------
# Presión QFE
# ----------------------------------


Presion_QFE_380029 <- read_delim(
  file = "C:/Users/benja/Desktop/bases/380029_XXXX_PresionQFE_.csv",
  delim = ";",
  col_names = FALSE,
  skip = 1,
  locale = locale(decimal_mark = ",", encoding = "Latin1"),
  show_col_types = FALSE
) %>%
  rename(
    CodigoNacional = X1,
    momento_raw    = X2,
    QFE_Valor      = X3
  ) %>%
  mutate(
    momento = ymd_hms(momento_raw, tz = "UTC"),
    fecha   = as_date(momento)
  ) %>%
  select(CodigoNacional, momento, fecha, QFE_Valor)


# Promedio diario de QFE
promedios_QFE <- Presion_QFE_380029 %>%
  group_by(fecha) %>%
  summarise(promedio_QFE = mean(QFE_Valor, na.rm = TRUE), .groups = "drop")

datos <- datos %>%
  mutate(
    fecha = as_date(momento)  # no volver a parsear momento
  ) %>%
  left_join(Presion_QFE_380029 %>% select(momento, QFE_Valor), by = "momento") %>%
  left_join(promedios_QFE, by = "fecha") %>%
  mutate(Presion_QFE = coalesce(QFE_Valor, promedio_QFE)) %>%
  select(-QFE_Valor, -promedio_QFE)


# ----------------------------------
# Temperatura
# ----------------------------------
Temperatura_380029 <- read_delim(
  "C:/Users/benja/Desktop/bases/380029_XXXX_Temperatura_.csv",
  delim = ";",
  col_names = FALSE,
  skip = 1,
  show_col_types = FALSE,
  locale = locale(decimal_mark = ",", encoding = "Latin1")
) %>%
  rename(
    CodigoNacional = X1,
    momento_raw    = X2,
    TEMP_Valor     = X3
  ) %>%
  mutate(
    momento = ymd_hms(momento_raw, tz = "UTC"),
    fecha   = as_date(momento),
    hora    = hour(momento)
  ) %>%
  select(CodigoNacional, momento, fecha, hora, TEMP_Valor)


# Unir temperatura
datos <- datos %>%
  mutate(hora = hour(momento)) %>%
  left_join(Temperatura_380029 %>% select(momento, TEMP_Valor), by = "momento") %>%
  rename(TEMP_exact = TEMP_Valor) %>%
  rowwise() %>%
  mutate(
    TEMP_neighbor = if (is.na(TEMP_exact)) {
      rango_ini <- fecha - days(5)
      rango_fin <- fecha + days(5)
      vals <- Temperatura_380029 %>%
        filter(hora == hora, fecha >= rango_ini, fecha <= rango_fin) %>%
        pull(TEMP_Valor)
      mean(vals, na.rm = TRUE)
    } else {
      NA_real_
    }
  ) %>%
  ungroup() %>%
  mutate(Temperatura_qfe = coalesce(TEMP_exact, TEMP_neighbor)) %>%
  select(-fecha, -hora, -TEMP_exact, -TEMP_neighbor)

# ----------------------------------
# Presión QFF
# ----------------------------------
Presion_QFF_380029 <- read_delim(
  "C:/Users/raul_/OneDrive - alu.ucm.cl/Universidad/Primer Semestre 2025/Series de Tiempo/Proyecto final/Bases originales/220002_XXXX_PresionQFF_.csv",
  delim = ";",
  col_names = FALSE,
  skip = 1,
  show_col_types = FALSE,
  locale = locale(decimal_mark = ",", encoding = "Latin1")
) %>%
  rename(
    CodigoNacional = X1,
    momento_raw    = X2,
    QFF_Valor      = X3
  ) %>%
  mutate(
    momento = ymd_hms(momento_raw, tz = "UTC"),
    fecha   = as_date(momento)
  ) %>%
  select(CodigoNacional, momento, fecha, QFF_Valor)

# Promedio diario QFF
promedios_QFF <- Presion_QFF_380029 %>%
  group_by(fecha) %>%
  summarise(promedio_QFF = mean(QFF_Valor, na.rm = TRUE), .groups = "drop")

# Unir QFF a datos
datos <- datos %>%
  mutate(fecha = as_date(momento)) %>%
  left_join(Presion_QFF_380029 %>% select(momento, QFF_Valor), by = "momento") %>%
  left_join(promedios_QFF, by = "fecha") %>%
  mutate(Presion_QFF = coalesce(QFF_Valor, promedio_QFF)) %>%
  select(-QFF_Valor, -promedio_QFF, -fecha)

# ----------------------------------
# Imputación de fechas faltantes
# ----------------------------------
datos <- datos %>%
  mutate(
    orig_order = row_number(),
    momento    = as.POSIXct(momento, tz = "UTC")
  )

n <- nrow(datos)
missing_idx <- which(is.na(datos$momento))

for (i in missing_idx) {
  prev_time <- if (i > 1) datos$momento[i - 1] else as.POSIXct(NA)
  next_time <- if (i < n) datos$momento[i + 1] else as.POSIXct(NA)
  
  dt <- if (!is.na(prev_time) && !is.na(next_time)) {
    as.numeric(difftime(next_time, prev_time, units = "hours"))
  } else {
    NA_real_
  }
  
  if (!is.na(dt) && dt >= 2) {
    datos$momento[i] <- prev_time + hours(1)
  } else if (!is.na(prev_time)) {
    datos$momento[i] <- prev_time + hours(1)
  } else if (!is.na(next_time)) {
    datos$momento[i] <- next_time - hours(1)
  }
}

datos <- datos %>%
  arrange(orig_order) %>%
  select(-orig_order)

# ----------------------------------
# Imputación por vecino más cercano
# ----------------------------------
vars_a_imputar <- c("dd_Valor", "ff_Valor", "VRB_Valor", "Presión_QFE", "Temperatura", "Presion_QFF")

for (var in vars_a_imputar) {
  na_idx <- which(is.na(datos[[var]]))
  
  for (i in na_idx) {
    prev_val <- if (i > 1) datos[[var]][i - 1] else NA
    next_val <- if (i < nrow(datos)) datos[[var]][i + 1] else NA
    
    datos[[var]][i] <- if (!is.na(prev_val)) {
      prev_val
    } else if (!is.na(next_val)) {
      next_val
    } else {
      NA
    }
  }
}


# ----------------------------------
# Exportar resultado final
# ----------------------------------
setwd("C:/Users/raul_/OneDrive - alu.ucm.cl/Universidad/Primer Semestre 2025/Series de Tiempo/Proyecto final")
write_xlsx(datos, path = "datos_220002.xlsx")



## ver datos faltantes

library(readxl)
df <- read_excel("datos_220002.xlsx")

colSums(is.na(df))
