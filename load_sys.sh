#!/bin/bash
echo "   ----------------------------------"
echo "   ==> Inicio Ejecución proceso memory: "$(date +"%Y-%m-%d %H:%M:%S")
## SE VALIDA DE NUEVO LA RUTA
echo "   ==> Validación ruta exerna"
echo $ruta_externa
if [[ -f "$ruta_externa" ]]
then
    echo "   ==> EJECUCIÓN PYTHON"
    echo "   ==> Lectura de variables archivo config.ini"
    ## LEEMOS ARCHIVO CONFIG.INI
    archivo_config="config.ini"
    ## VALOR DIRECTORIO
    directorio=$(grep "^directorio=" "$archivo_config" | cut -d'=' -f2)
    ## EJECUCIÓN DEL PYTHON
    python3 $directorio/main.py $ruta_externa
else
    echo "   ==> El archivo no existe dentro del directorio"
fi
echo "   ==> Fin Ejecución proceso memory"
echo "   ----------------------------------"