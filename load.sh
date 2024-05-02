#!/bin/bash
echo "==================PROCESO EJECUCIÓN INICIADO=================="
echo "==> Proceso iniciado: "$(date +"%Y-%m-%d %H:%M:%S")
# ------------------------------------------ VARIABLES -------------------------------------------
echo "==> Lectura de variables archivo config.ini"
## LEEMOS ARCHIVO CONFIG.INI
archivo_config="config.ini"
## VALOR DIRECTORIO
directorio=$(grep "^directorio=" "$archivo_config" | cut -d'=' -f2)
## VALOR DATA
data=$(grep "^data=" "$archivo_config" | cut -d'=' -f2)
# ---------------------------------------- FIN VARIABLES -----------------------------------------#

if [ $# -eq 0 ]; then
    ## =======
    ## DEFINIR LA LECTURA DE ARCHIVOS
    echo "==> Lectura de ARCHIVOS: "
    for archivo in "$directorio$data"/*; do
        echo "==> Inicio de PROCESO "
        echo "============ Ejecución ARCHIVO:"$archivo" ============"
        ## ESTABLECER SI CONTIENE EL DIRECTORIO CON ARVCHIVOS la cadena "2012-"
        if echo "$archivo" | grep -q "2012-"; then                
            echo "   ==> Envío de ruta a ejecución en proceso"
            ruta_externa=$archivo        
            export ruta_externa
            /bin/bash $directorio"/load_sys.sh"
            echo ""
            echo ""
            echo ""
            echo ""
            echo ""
        else
            echo "   ==> El directorio no contiene la cadena '2012-'."
        fi
    done
else
    if [ "$1" = "validation.csv" ] ; then
        ## =======
        ## DEFINIR LA LECTURA DE ARCHIVOS
        echo "==> Lectura de ARCHIVO: "
        echo "==> Inicio de PROCESO "
        archivo=$1
        echo "============ Ejecución ARCHIVO:"$archivo" ============"
        echo "   ==> Envío de ruta a ejecución en proceso"
        ruta_externa=$directorio$data"/"$archivo
        echo $ruta_externa
        export ruta_externa
        /bin/bash $directorio"/load_sys.sh"
        echo ""
        echo ""
        echo ""
        echo ""
        echo ""    
    fi
fi 

