const { contextBridge, ipcRenderer } = require('electron');

contextBridge.exposeInMainWorld('api', {
  // Oracle
  connectOracle: cfg => ipcRenderer.invoke('connect-oracle', cfg),
  disconnectOracle: () => ipcRenderer.invoke('disconnect-oracle'),
  listTables: () => ipcRenderer.invoke('list-tables'),
  listColumns: table => ipcRenderer.invoke('list-columns', table),
  // BigQuery
  connectBQ: cfg => ipcRenderer.invoke('connect-bq', cfg),
  disconnectBQ: () => ipcRenderer.invoke('disconnect-bq'),
  bqListTables: () => ipcRenderer.invoke('bq-list-tables'),
  bqListColumns: table => ipcRenderer.invoke('bq-list-columns', table),
  comparePairs: pairs => ipcRenderer.invoke('compare-pairs', pairs)

});



