# BSC Lifecycle Observatory

Recolector público de investigación, sin billeteras ni operaciones de trading. Registra eventos originales de contratos documentados de Four.meme Classic V2, Flap y fábricas de PancakeSwap V2/V3, y sigue los tokens y pools observados. Conserva respuestas crudas de lecturas públicas de mercado y seguridad.

Los datos y el estado se archivan en Releases diarias con hashes. La rama `lifecycle-state` contiene el puntero al último archivo verificado. Una sesión nueva continúa desde los cursores guardados; los datos históricos recuperados conservan su hora de recepción y no se presentan como observaciones en vivo.

La recogida usa sesiones limitadas con continuación y cron de recuperación. No garantiza disponibilidad 24/7, cobertura de toda BSC ni rentabilidad. Los saldos reconstruidos con eventos y los informes de terceros no certifican capacidad de venta. OpenFour, PancakeSwap Infinity y contratos no incluidos en `contracts.json` no se afirman cubiertos.

`experiments/lifecycle/control.json` permite detener futuras sesiones. Para detener una sesión actual se cancela su ejecución en Actions. No hay claves privadas, señales de inversión ni ejecución financiera.
