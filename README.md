# **Proyecto 4 Datalab Flight Delay and Cancellation**

**Ficha Técnica: Proyecto 4 Datalab**

**Título del Proyecto**

**Proyecto 4 Datalab Flight Delay and Cancellation**

**Objetivo**

Desarrollar un score de riesgo para los vuelos mediante el análisis de datos y la evaluación del riesgo relativo, que permita clasificar los vuelos en distintas categorías de riesgo basadas en su probabilidad de retraso. Esta clasificación ayudará a las aerolíneas a tomar decisiones informadas sobre cancelaciones y a reducir los riesgos de retraso en sus operaciones. Además, la integración de métricas existentes sobre el estado de los vuelos fortalecerá el modelo, mejorando su capacidad para identificar riesgos y, en última instancia, gestionar eficazmente los retrasos.


**Equipo**

**Herramientas y Tecnologías**

Listado de herramientas y tecnologías utilizadas en el proyecto:

1) Big Query (SQL).
2) Google Colab (Python).
3) Workspace Google (Presentaciones, Chat GPT y Documentos).
4) Videos Looms.
5) Git Hub.


**Procesamiento y análisis**
**Proceso de Análisis de Datos:**

**1.1 Procesar y preparar base de datos**

1) Identificar y manejar valores nulos.
2) Identificar y manejar valores duplicados.
3) Identificar y manejar datos fuera del alcance del análisis.
4) Identificar y manejar datos discrepantes en variables categóricas.
5) Identificar y manejar datos discrepantes en variables numéricas.
6) Comprobar y cambiar tipo de dato.
7) Unir tablas.
8) Crear nuevas variables.
9) Construir tablas auxiliares.


A continuación ejemplos de lo anterior:





```
WITH MedianValues AS (
  SELECT
    PERCENTILE_CONT(delay_due_carrier, 0.5) OVER() AS median_delay_due_carrier,
    PERCENTILE_CONT(delay_due_weather, 0.5) OVER() AS median_delay_due_weather,
    PERCENTILE_CONT(delay_due_nas, 0.5) OVER() AS median_delay_due_nas,
    PERCENTILE_CONT(delay_due_security, 0.5) OVER() AS median_delay_due_security,
    PERCENTILE_CONT(delay_due_late_aircraft, 0.5) OVER() AS median_delay_due_late_aircraft,
    PERCENTILE_CONT(dep_delay, 0.5) OVER() AS median_dep_delay,
    PERCENTILE_CONT(taxi_out, 0.5) OVER() AS median_taxi_out,
    PERCENTILE_CONT(air_time, 0.5) OVER() AS median_air_time,
    PERCENTILE_CONT(arr_delay, 0.5) OVER() AS median_arr_delay,
    PERCENTILE_CONT(dep_time, 0.5) OVER() AS median_dep_time,
    PERCENTILE_CONT(wheels_off, 0.5) OVER() AS median_wheels_off,
    PERCENTILE_CONT(wheels_on, 0.5) OVER() AS median_wheels_on,
    PERCENTILE_CONT(taxi_in, 0.5) OVER() AS median_taxi_in,
    PERCENTILE_CONT(arr_time, 0.5) OVER() AS median_arr_time,
    PERCENTILE_CONT(elapsed_time, 0.5) OVER() AS median_elapsed_time
  FROM
    `Dataset.view_flights_202301`
  LIMIT 1 -- Asegurarse de que solo calculamos y recuperamos una fila.
),
-- Usar una subconsulta para integrar los valores de la mediana en la consulta principal.
Data AS (
  SELECT
    C.dot_code,
    C.fl_date,
    C.airline_code,
    C.fl_number,
    C.origin,
    C.origin_city,
    C.dest,
    C.dest_city,
    C.crs_dep_time,
    COALESCE(C.dep_time, (SELECT median_dep_time FROM MedianValues)) AS dep_time,
    COALESCE(C.dep_delay, (SELECT median_dep_delay FROM MedianValues)) AS dep_delay,
    COALESCE(C.taxi_out, (SELECT median_taxi_out FROM MedianValues)) AS taxi_out,
    COALESCE(C.wheels_off, (SELECT median_wheels_off FROM MedianValues)) AS wheels_off,
    COALESCE(C.wheels_on, (SELECT median_wheels_on FROM MedianValues)) AS wheels_on,
    COALESCE(C.taxi_in, (SELECT median_taxi_in FROM MedianValues)) AS taxi_in,
    C.crs_arr_time,
    COALESCE(C.arr_time, (SELECT median_arr_time FROM MedianValues)) AS arr_time,
    GREATEST(COALESCE(C.arr_delay, (SELECT median_arr_delay FROM MedianValues)), 0) AS arr_delay,
    CASE WHEN C.arr_delay > 0 THEN 1 ELSE 0 END AS arr_delay_calculated,
    C.cancelled,
    C.cancellation_code,
    C.crs_elapsed_time,
    COALESCE(C.elapsed_time, (SELECT median_elapsed_time FROM MedianValues)) AS elapsed_time,
    COALESCE(C.air_time, (SELECT median_air_time FROM MedianValues)) AS air_time,
    C.distance,
    -- Lógica condicional para arr_delay <= 0
    CASE
      WHEN C.arr_delay <= 0 THEN 0
      ELSE COALESCE(C.delay_due_carrier, (SELECT median_delay_due_carrier FROM MedianValues))
    END AS delay_due_carrier,
    CASE
      WHEN C.arr_delay <= 0 THEN 0
      ELSE COALESCE(C.delay_due_weather, (SELECT median_delay_due_weather FROM MedianValues))
    END AS delay_due_weather,
    CASE
      WHEN C.arr_delay <= 0 THEN 0
      ELSE COALESCE(C.delay_due_nas, (SELECT median_delay_due_nas FROM MedianValues))
    END AS delay_due_nas,
    CASE
      WHEN C.arr_delay <= 0 THEN 0
      ELSE COALESCE(C.delay_due_security, (SELECT median_delay_due_security FROM MedianValues))
    END AS delay_due_security,
    CASE
      WHEN C.arr_delay <= 0 THEN 0
      ELSE COALESCE(C.delay_due_late_aircraft, (SELECT median_delay_due_late_aircraft FROM MedianValues))
    END AS delay_due_late_aircraft,
    C.fl_year,
    C.fl_month,
    C.fl_day,
    C.ruta,
    D.description_limpio
  FROM
    `Dataset.view_dot_code_limpio` D
  LEFT JOIN
    `Dataset.view_flights_202301` C ON C.dot_code = D.dot_code
  WHERE
    C.dot_code IS NOT NULL
    AND D.description_limpio IS NOT NULL
)
SELECT * FROM Data;

  
```










**1.2 Análisis exploratorio**

1) Agrupar datos según variables categóricas.
2) Visualizar las variables categóricas.
3) Aplicar medidas de tendencia central.
4) Visualizar distribución.
5) Aplicar medidas de dispersión.
6) Calcular cuartiles, deciles o percentiles.
7) Calcular correlación entre variables.


A continuación ejemplos de lo anterior:





```
WITH MedianValues AS (
  SELECT
    PERCENTILE_CONT(delay_due_carrier, 0.5) OVER() AS median_delay_due_carrier,
    PERCENTILE_CONT(delay_due_weather, 0.5) OVER() AS median_delay_due_weather,
    PERCENTILE_CONT(delay_due_nas, 0.5) OVER() AS median_delay_due_nas,
    PERCENTILE_CONT(delay_due_security, 0.5) OVER() AS median_delay_due_security,
    PERCENTILE_CONT(delay_due_late_aircraft, 0.5) OVER() AS median_delay_due_late_aircraft,
    PERCENTILE_CONT(dep_delay, 0.5) OVER() AS median_dep_delay,
    PERCENTILE_CONT(taxi_out, 0.5) OVER() AS median_taxi_out,
    PERCENTILE_CONT(air_time, 0.5) OVER() AS median_air_time,
    PERCENTILE_CONT(arr_delay, 0.5) OVER() AS median_arr_delay,
    PERCENTILE_CONT(dep_time, 0.5) OVER() AS median_dep_time,
    PERCENTILE_CONT(wheels_off, 0.5) OVER() AS median_wheels_off,
    PERCENTILE_CONT(wheels_on, 0.5) OVER() AS median_wheels_on,
    PERCENTILE_CONT(taxi_in, 0.5) OVER() AS median_taxi_in,
    PERCENTILE_CONT(arr_time, 0.5) OVER() AS median_arr_time,
    PERCENTILE_CONT(elapsed_time, 0.5) OVER() AS median_elapsed_time
  FROM
    `Dataset.view_flights_202301`
  LIMIT 1 -- Asegurarse de que solo calculamos y recuperamos una fila.
),
-- Usar una subconsulta para integrar los valores de la mediana en la consulta principal.
Data AS (
  SELECT
    C.dot_code,
    C.fl_date,
    C.airline_code,
    C.fl_number,
    C.origin,
    C.origin_city,
    C.dest,
    C.dest_city,
    C.crs_dep_time,
    COALESCE(C.dep_time, (SELECT median_dep_time FROM MedianValues)) AS dep_time,
    COALESCE(C.dep_delay, (SELECT median_dep_delay FROM MedianValues)) AS dep_delay,
    COALESCE(C.taxi_out, (SELECT median_taxi_out FROM MedianValues)) AS taxi_out,
    COALESCE(C.wheels_off, (SELECT median_wheels_off FROM MedianValues)) AS wheels_off,
    COALESCE(C.wheels_on, (SELECT median_wheels_on FROM MedianValues)) AS wheels_on,
    COALESCE(C.taxi_in, (SELECT median_taxi_in FROM MedianValues)) AS taxi_in,
    C.crs_arr_time,
    COALESCE(C.arr_time, (SELECT median_arr_time FROM MedianValues)) AS arr_time,
    GREATEST(COALESCE(C.arr_delay, (SELECT median_arr_delay FROM MedianValues)), 0) AS arr_delay,
    CASE WHEN C.arr_delay > 0 THEN 1 ELSE 0 END AS arr_delay_calculated,
    C.cancelled,
    C.cancellation_code,
    C.crs_elapsed_time,
    COALESCE(C.elapsed_time, (SELECT median_elapsed_time FROM MedianValues)) AS elapsed_time,
    COALESCE(C.air_time, (SELECT median_air_time FROM MedianValues)) AS air_time,
    C.distance,
    -- Lógica condicional para arr_delay <= 0
    CASE
      WHEN C.arr_delay <= 0 THEN 0
      ELSE COALESCE(C.delay_due_carrier, (SELECT median_delay_due_carrier FROM MedianValues))
    END AS delay_due_carrier,
    CASE
      WHEN C.arr_delay <= 0 THEN 0
      ELSE COALESCE(C.delay_due_weather, (SELECT median_delay_due_weather FROM MedianValues))
    END AS delay_due_weather,
    CASE
      WHEN C.arr_delay <= 0 THEN 0
      ELSE COALESCE(C.delay_due_nas, (SELECT median_delay_due_nas FROM MedianValues))
    END AS delay_due_nas,
    CASE
      WHEN C.arr_delay <= 0 THEN 0
      ELSE COALESCE(C.delay_due_security, (SELECT median_delay_due_security FROM MedianValues))
    END AS delay_due_security,
    CASE
      WHEN C.arr_delay <= 0 THEN 0
      ELSE COALESCE(C.delay_due_late_aircraft, (SELECT median_delay_due_late_aircraft FROM MedianValues))
    END AS delay_due_late_aircraft,
    C.fl_year,
    C.fl_month,
    C.fl_day,
    C.ruta,
    D.description_limpio
  FROM
    `Dataset.view_dot_code_limpio` D
  LEFT JOIN
    `Dataset.view_flights_202301` C ON C.dot_code = D.dot_code
  WHERE
    C.dot_code IS NOT NULL
    AND D.description_limpio IS NOT NULL
)
SELECT * FROM Data;
 


  
```





![correlacion due late aircraft y arr delay](https://github.com/user-attachments/assets/c9d1cf2e-85ca-469d-a020-745569f7210f)










![analisis distribución](https://github.com/user-attachments/assets/6701e75b-ea2e-4a5f-b8f1-cae5af52e599)






**1.3 Aplicar técnica de análisis**

1) Aplicar segmentación.
2) Calcular riesgo relativo.
3) Validar hipótesis.



A continuación ejemplos de lo anterior:





```
SELECT
  dot_code,
  fl_date,
  airline_code,
  fl_number,
  origin,
  origin_city,
  dest,
  dest_city,
  crs_dep_time,
  dep_time,
  dep_delay,
  taxi_out,
  wheels_off,
  wheels_on,
  taxi_in,
  crs_arr_time,
  arr_time,
  arr_delay,
  arr_delay_calculated,
  cancelled,
  cancellation_code,
  crs_elapsed_time,
  elapsed_time,
  air_time,
  distance,
  delay_due_carrier,
  delay_due_weather,
  delay_due_nas,
  delay_due_security,
  delay_due_late_aircraft,
  fl_year,
  fl_month,
  fl_day,
  ruta,
  description_limpio,
  arr_delay_winsorized,
  air_time_winsorized,
  distance_winsorized,
  delay_due_carrier_winsorized,
  delay_due_weather_winsorized,
  delay_due_nas_winsorized,
  delay_due_security_winsorized,
  delay_due_late_aircraft_winsorized,
  crs_elapsed_time_winsorized,
  elapsed_time_winsorized,
  dep_delay_winsorized,
  Q1_dep_delay_winsorized,
  Q2_dep_delay_winsorized,
  Q3_dep_delay_winsorized,
  Q1_arr_delay_winsorized,
  Q2_arr_delay_winsorized,
  Q3_arr_delay_winsorized,
  Q1_delay_due_carrier_winsorized,
  Q2_delay_due_carrier_winsorized,
  Q3_delay_due_carrier_winsorized,
  Q1_delay_due_weather_winsorized,
  Q2_delay_due_weather_winsorized,
  Q3_delay_due_weather_winsorized,
  Q1_delay_due_nas_winsorized,
  CASE
    WHEN delay_due_carrier_winsorized >= 3 AND delay_due_carrier_winsorized <= 108.63999 THEN 1
    ELSE 0
END
  AS dummy_delay_due_carrier_wins,
  CASE
    WHEN delay_due_weather_winsorized > 0  AND delay_due_weather_winsorized<= 11 THEN 1
    ELSE 0
END
  AS dummy_due_weather_wins,
  CASE
    WHEN delay_due_nas_winsorized > 0 AND delay_due_nas_winsorized <= 1 THEN 1
    ELSE 0
END
  AS dummy_delay_due_nas_wins,
  CASE
    WHEN delay_due_security_winsorized = 0 THEN 1
    ELSE 0
END
  AS dummy_delay_due_security_wins,
  CASE
    WHEN delay_due_late_aircraft_winsorized > 0 AND delay_due_late_aircraft_winsorized <=128 THEN 1
    ELSE 0
END
  AS dummy_delay_due_late_aircraft_wins
FROM
  `Dataset.view_union_general_I`



  
```





```

WITH Cuartiles AS (
    SELECT
        delay_due_late_aircraft_winsorized,
        arr_delay_calculated,
        NTILE(4) OVER (ORDER BY delay_due_late_aircraft_winsorized) AS cuartil
    FROM
        Dataset.view_union_general_I
),
ConteoPorCuartil AS (
    SELECT
        cuartil,
        arr_delay_calculated,
        COUNT(*) AS cantidad,
        MIN(delay_due_late_aircraft_winsorized) AS min_delay_due_late_aircraft_winsorized,
        MAX(delay_due_late_aircraft_winsorized) AS max_delay_due_late_aircraft_winsorized 
    FROM
        Cuartiles
    GROUP BY
        cuartil,
        arr_delay_calculated
),
RiskCalculation AS (
    SELECT
        cuartil,
        MIN(min_delay_due_late_aircraft_winsorized) AS rango_min_delay_due_late_aircraft_winsorized,
        MAX(max_delay_due_late_aircraft_winsorized) AS rango_max_delay_due_late_aircraft_winsorized,
        SUM(CASE WHEN arr_delay_calculated = 1 THEN cantidad ELSE 0 END) AS arr_delay_cal_count,
        SUM(CASE WHEN arr_delay_calculated = 0 THEN cantidad ELSE 0 END) AS non_arr_delay_cal_count,
        SUM(cantidad) AS total_count,
        SUM(CASE WHEN arr_delay_calculated = 1 THEN cantidad ELSE 0 END) * 1.0 / SUM(cantidad) AS risk
    FROM
        ConteoPorCuartil
    GROUP BY
        cuartil
),
RelativeRisk AS (
    SELECT
        q2.cuartil AS non_exposed_quartile,
        q2.risk AS non_exposed_risk,
        q1.risk AS exposed_risk,
        q1.risk / q2.risk AS risk_relative_delay_due_late_aircraft
    FROM
        RiskCalculation q1
    CROSS JOIN
        RiskCalculation q2
    WHERE
        q1.cuartil = 1 -- Cuartil expuesto
)
SELECT
    r.non_exposed_quartile,
    r.risk_relative_delay_due_late_aircraft,
    rc.rango_min_delay_due_late_aircraft_winsorized,
    rc.rango_max_delay_due_late_aircraft_winsorized,
    rc.arr_delay_cal_count AS cantidad_arr_delay_cal_1,
    rc.non_arr_delay_cal_count AS cantidad_arr_delay_cal_0
FROM
    RelativeRisk r
JOIN
    RiskCalculation rc
ON
    r.non_exposed_quartile = rc.cuartil
ORDER BY
    non_exposed_quartile;



  
```







```

SELECT
*,
CASE WHEN (dummy_delay_due_carrier_wins  + dummy_delay_due_late_aircraft_wins +  dummy_delay_due_nas_wins + dummy_due_weather_wins +  dummy_delay_due_security_wins)  = 3 THEN 'delay' ELSE 'no delay' END AS clasificacion
 FROM `Dataset.view_dummy_1`



  
```








![Frecuencia de retraso por ruta](https://github.com/user-attachments/assets/ce8862d9-b4f8-48c4-82f4-854bf1a225a2)









![promedio arr delay por ruta](https://github.com/user-attachments/assets/af0201af-7eb4-42cb-91ab-b09951deee64)








![Delay](https://github.com/user-attachments/assets/995dce9d-dbc6-4107-8b41-55a52876e01a)








![Total arr delay por ruta](https://github.com/user-attachments/assets/010cec4c-feb7-494e-9a33-107a26ddb5e8)








![Estadode vuelo](https://github.com/user-attachments/assets/339f48cf-5f57-48b1-8374-be68897b46cf)









**Resultados y Conclusiones**

Durante el desarrollo del proyecto se utilizó el proceso de Análisis de Datos, cálculo de riesgo relativo y la metodología  de segmentación (Cuartiles) para validación  de hipótesis. Se plantearon seis hipótesis, para saber qué hace que un vuelo presente retraso. Estas hipótesis se detallan a continuación y de esto se puede concluir lo siguiente:

I) Existen rutas que presentan una mayor frecuencia de retrasos en comparación con otras.
Se valida la hipótesis con los resultados obtenidos en Figura N° 1:  Frecuencia de retrasos por ruta (arr_delay_winsorized) que muesta que la ruta LGA to ORD tiene una frecuencia de 147 retrasops en el periodo analizado.

II) Es posible calcular el tiempo promedio de retraso por ruta utilizando las variables disponibles.
Se valida la hipótesis con los resultados de la tabla N° 1 podemos indicar que existen 4 rutas que presentan el mayor valor promedio de retraso (AUS to SRQ, BOS to VPS, FLL to PIE y LGA to MTJ. y el valor promedio de retraso de la muestra es de 14.33 minutos. 

III) Con los datos disponibles, se puede identificar el principal motivo que genera los retrasos en los vuelos.
Se valida la hipótesis con los resultados de la Figura  N° 3:  Motivos que generan retraso por ruta, siendo late_aircraft el principal motivo que genera los retrasos con un 39,21 %.

V) La Regresión Lineal permite predecir de manera efectiva el tiempo de retraso de un vuelo.
No es posible validar esta hipótesis dado que segùn la Figura N° 5:  Correlación entre variables delay_due_late_aircraft y delay_due_late_aircraft, su correlaciòn es de 0, 6578.Sin embargo El modelo tiene un rendimiento deficiente en la clasificación de la Clase 0 (No Delay), pero un rendimiento muy bueno en la clasificación de la Clase 1 (Delay).

VI) La Regresión Logística es una herramienta adecuada para determinar el estado (a tiempo o retrasado) de un vuelo.
Se valida esta Hipótesis con los resultados de la Tabla N° 3: Métricas de matriz de confusión de regresión Logística, que nos indica que Las métricas proporcionadas muestran un modelo de regresión logística con un rendimiento muy alto en ambas clases.  La Clase 0 (No Delay) y la Clase 1 (Delay).
Este modelo de regresión logística (RL) está logrando un rendimiento sobresaliente tanto en la clase "No Delay" como en la clase "Delay". Tiene una excelente capacidad para identificar correctamente ambas clases con muy pocos errores, como lo reflejan los altos valores de precisión, recall y F1 score. La exactitud del 98% es también muy indicativa de un modelo que generaliza bien y tiene un muy bajo número de errores. Sin embargo, es importante monitorear cualquier sesgo de clase, aunque aquí parece estar bien balanceado.
En general, el modelo de RL parece estar bien estructurado para reflejar los factores clave que influyen en los retrasos, y resalta cómo los problemas externos (como el clima y las operaciones del NAS) juegan un papel fundamental, se recomienda su uso.





**Limitaciones/Próximos Pasos**
Sin observaciones.




**Enlaces de interés**


[Proyecto_4_Datalab](https://drive.google.com/file/d/1_JNNFyJtzMwxkpl24USF3mDC1HXP2Gv4/view?usp=sharing)

[Proyecto 4 Datalab Flight Delay and Cancellation ppt](https://docs.google.com/presentation/d/1rH3OvMgWFa5iuzLW4DdCoozva4LseB95H4d5sgiqMNQ/edit?usp=sharing)

[Proyecto 4 Datalab Flight Delay and Cancellation GC](https://colab.research.google.com/drive/1FaDdWMeO48EyQ5nrSGfSqKIvjGdn5p2Y?usp=sharing)

[Presentación Video](https://www.loom.com/share/36a25845d04a4a77b93e69b43b5521bb?sid=3b273b73-2a71-4e89-81c8-7e35115fc048)








