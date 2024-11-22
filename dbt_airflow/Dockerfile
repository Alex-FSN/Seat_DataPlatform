FROM quay.io/astronomer/astro-runtime:10.4.0
#http://pac.seat.vwgroup.com/seat.es/seatCodeProxypac.pac
# Puerto a exponer
EXPOSE 80

# Configurar variables de entorno para el proxy
ENV HTTP_PROXY=http://10.82.32.254:80
ENV HTTPS_PROXY=http://10.82.32.254:80

# Crear y activar un entorno virtual de Python, instalar dbt-snowflake usando el proxy y luego desactivar el entorno virtual
RUN python -m venv dbt_venv && \
    source dbt_venv/bin/activate && \
    pip install --trusted-host pypi.org \
                --trusted-host pypi.python.org \
                --trusted-host files.pythonhosted.org \
                --proxy http://10.82.32.254:80 \
                --no-cache-dir dbt-snowflake && \
    deactivate