require('./config').readConfig()

const db = require('./db')
const logger = require('./logger').logger

async function init() { 
  let DB = require('./db-graph')
  let graph = new DB({
    'campaign': {
      pk: ['id'],
      columns: ['landing_url', 'fallback_url', 'status'], 
      lastUpdatedColumn: 'updated_at',
      idMaps: ['foreign_id']
    }, 
    'campaign_line': {
      pk: ['campaign_id', 'affiliate_id'],
      columns: ['line_url', 'report_category_id'],
      lastUpdatedColumn: 'updated_at',
      fk: {
        'campaign_id': 'campaign.id'
      }
    },
    'campaign_product': {
      pk: ['campaign_id', 'product_id'],
      columns: [],
      lastUpdatedColumn: 'updated_at',
      fk: {
        'campaign_id': 'campaign.id'
      }
    }    
  }, {
    updateInterval: 15000
  })
  await graph.init()
  console.log(JSON.stringify(graph, null, 2))
}

init()
