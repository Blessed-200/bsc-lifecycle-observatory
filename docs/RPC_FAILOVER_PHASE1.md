# BSC RPC failover, fase 1

## Diagnóstico probado

El recolector YA tenía tres URLs públicas, pero las leía en un orden fijo y abortaba cuando las tres quedaban temporalmente indisponibles. Los logs de GitHub Actions mostraron `ReadError("all RPC providers unavailable or cooling down")` y `RuntimeError("sources unavailable for 12 cycles; data retained")`.

## Cambio propuesto (solo lectura)

- `RPCPool`: round-robin, comprobación `eth_chainId == 0x38` antes del primer uso, enfriamientos individuales, espera exponencial y failover inmediato al siguiente proveedor.
- Errores de rango grande: se propagan al recolector para dividir `eth_getLogs`. Nunca se convierten en rangos vacíos.
- Cuando todos fallan, el lector lanza un error; no genera bloques ni logs ficticios. El ciclo de recolección continúa en modo degradado y el cursor no avanza si falla `scan`.
- Los endpoints completos pueden contener tokens secretos. Solo se exponen sus hosts en `RPCPool.status()`; el recolector existente registra el hash de cada URL y su respuesta HTTP sin registrar el path del endpoint RPC en la entrada RPC.
- `BSC_RPC_URLS`: lista de 3 o más URLs HTTPS separadas por comas. Guardar en el secreto de Actions **BSC_RPC_URLS**, nunca en un commit. Si está vacío, se mantiene la terna pública previa, que puede quedar indisponible.

## Verificación

`PYTHONPATH=src pytest tests/test_rpc_pool.py tests/test_lifecycle.py`

El nuevo módulo fue probado localmente con RPC falsos y sin red. Los CI de GitHub del PR validarán el contexto real del repositorio antes de cualquier merge. No se ha verificado todavía el rendimiento contra tres proveedores comerciales reales ni un SLA de disponibilidad 100 %.

## Límites actuales

1. El cursor por stream y su persistencia siguen en `Store.commit_range`, con comprobación de `blockHash` y publicación del archivo. Esta fase no modifica el formato del estado ni los parsers.
2. No hay recolección universal de todos los contratos BSC: la cobertura se limita a los descubrimientos y streams configurados.
3. La ausencia simultánea de todos los RPC sigue produciendo demora; no se puede garantizar 0 interrupciones ni 0 huecos de observación en tiempo real.
4. La misión 2 (contratos, reservas V2/V3, detección MEV, simulación de venta) y la auditoría ampliada de misión 3 NO forman parte de este PR.

## Despliegue recomendado

1. Configurar al menos tres proveedores independientes de BSC, preferentemente con credenciales y cuotas separadas; validar `eth_getLogs`, `eth_call` y datos históricos.
2. Ejecutar CI completo del PR y revisar que los 3 proveedores responden con `chainId=56`.
3. Prueba de recuperación con un proveedor bloqueado y otro que responda 429; observar cobertura de bloques y checkpoints.
4. Merge después de revisión de seguridad y prueba de carga; no reiniciar con `--bootstrap` sobre estados existentes.


## Limitación documentada por BNB Chain

La documentación oficial BSC Mainnet (https://docs.bnbchain.org/bnb-smart-chain/developers/json_rpc/json-rpc-endpoint/) indica que `eth_getLogs` está deshabilitado en sus endpoints públicos BSC de la lista documentada. Por tanto no basta con configurar tres URLs distintas: **cada proveedor debe demostrar soporte real de `eth_getLogs` para la antigüedad y tamaño de rango requeridos**. El balanceador no puede sustituir disponibilidad histórica, capacidad contratada ni redundancia física. Registrar esa comprobación como requisito de despliegue para proveedores independientes (p.ej., QuickNode, Chainstack, Ankr) con credenciales proporcionadas por el usuario.
