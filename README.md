# HashTable_with_Zookeeper
Sistema distribuído gestionado con zookeeper para almacenar una HashTable. Tiene tolerancia a 1 fallo a través de una réplica de la tabla en servidores vecinos. 
Permite la restauración automática de un servidor caído al ser detectado con Zookeeper.

La aplicación funciona con los módulos *DHTMain* y *ServerManager*.

## DHTMain
Aplicación para el cliente, permite:
- Introducir valores en la tabla.
- Obtener valores.
- Borrar valores.

## ServerManager
Gestor de servidores, permite:
- Crear servidores.
- Borrar servidores por índice.
- Borrar servidor aleatorio.
- Activar la restauración automática de los servidores
- Mostrar las tablas de datos de los servidores.
