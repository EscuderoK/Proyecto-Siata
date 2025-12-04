Descripción del Proyecto
Este proyecto implementa una solución de Ingeniería de Datos para automatizar la gestión de información investigativa de la Universidad TIFAE. El sistema centraliza datos de investigadores, proyectos, grupos de investigación y ejecución financiera.

La solución utiliza un enfoque híbrido de datos:

Datos Reales: Ingesta automatizada desde el Portal de Datos Abiertos de Colombia (Minciencias y SGR).

Datos Sintéticos: Algoritmos de generación de datos para simular transacciones financieras y vinculaciones históricas.

🚀 Arquitectura Técnica
Modelo de Datos
Base de Datos: Relacional (MySQL Community Server).

Normalización: Tercera Forma Normal (3FN).

Estrategia de Herencia: Table-per-Type para la entidad Investigador (subtipos Profesor y Estudiante).

ETL de canalización (Extracción, Transformación, Carga)
El proceso de carga se realiza mediante scripts en Python que orquestan:

Extracción: Consumo de APIs JSON (Socrata) de fuentes gubernamentales.

Transformación: Limpieza con Pandas, estandarización de texto, manejo de nulos y generación de Mock Data.

Load: Carga masiva a MySQL utilizando SQLAlchemy con integridad referencial.


