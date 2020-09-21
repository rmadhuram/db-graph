const db = require('./db');
const { logger } = require('./logger');

// we are maintaining a map of new updates so that we can generate links only for updated entities.
var updatesMap = {}
var instanceCounter = 0

function convertToLCC(str) {
  return str.split('_').map((w, i) => i > 0 ?
    w.charAt(0).toUpperCase() + w.slice(1) : w)
    .join('')
}

function makePlural(str) {
  // temporary
  return str + 's'
}

class DBGraph {
  constructor(dbSpec, options) {
    this._spec = dbSpec
    this._options = options
    this._intervalTimer = null
    this._instanceId = instanceCounter++
    this._idMaps = {}

    // validate spec
    // pk should be defined
  }

  // private method
  async fetchEntities(isUpdate) {
    let spec = this._spec

    // clear updatesMap. We will add only the entities fetched in this pass to this map.
    updatesMap = {} 

    for (let entity of Object.keys(spec)) {
      let entitySpec = spec[entity] 
      logger.info(`Init: Fetching ${entity}`)

      let columns = [...entitySpec.pk, ...entitySpec.columns, ...entitySpec.idMaps || []] || '*'
      columns = columns.join(',')

      let stmt = `SELECT ${columns} from ${entity}`
      if (isUpdate) {
        let lastUpdatedColumn = entitySpec.lastUpdatedColumn || 'last_updated'
        let interval = this._options.updateWindowInSecs || 60

        // fetch updated entities only.
        stmt += ` WHERE ${lastUpdatedColumn} > DATE_SUB(CURRENT_TIMESTAMP, INTERVAL ${interval} second)`
      }

      logger.info(stmt)
      let results = await db.executeQuery(stmt)
      logger.info(`Fetched ${results.length} records`)

      let entityMap = {}
      results.forEach(result => {
        let key = entitySpec.pk.map(pk => result[pk]).join('|')
        entityMap[key] = result
      })

      this[entity] = Object.assign(this[entity] || {}, entityMap) // merge entityMap with existing
      updatesMap[entity] = entityMap

      if (entitySpec.idMaps) {
        for (const extId of entitySpec.idMaps) {
          let key = entity + '.' + extId
          this._idMaps[key] = this._idMaps[key] || {}
          let map = this._idMaps[key]
          results.forEach(result => { 
            map[result[extId]] = result
          })
        }
      }
    }  
  }

  // private method
  setupLinks() {
    let spec = this._spec

    // for each entity defined in the spec.
    for (let entity of Object.keys(spec)) {
      let fk = spec[entity]['fk']
      if (!fk) continue

      // for every foreign key mapping defined in this entity.
      // e.g: campaign_id: 'campaign.id'
      Object.keys(fk).forEach(key => {

        // this is of the form <entiity>.<field>, e.g: campaign.id
        let [fkEntityName, fkEntityField] = fk[key].split('.')
        //logger.info(`fkEntityName: ${fkEntityName} fkEntityField: ${fkEntityField}`)

        // if the foreign key entity map does not exist, log error and skip this.
        if (!this[fkEntityName]) {
          logger.error(`No entity found ${fkEntityName}`)
          return
        }

        let fkEntityMap = this[fkEntityName]
        let containerKey = makePlural(convertToLCC(entity))
        //console.log(JSON.stringify(fkEntityMap, null, 2))
        //logger.info(`fkEntityMap: ${fkEntityMap} containerKey: ${containerKey}`)


        let thisEntityMap = updatesMap[entity]  // Use current updates. 
        //console.log(JSON.stringify(thisEntityMap, null, 2))
        logger.info(`entity: ${entity}`)

        // iterate through all objects of this entity.
        for (let thisEntityKey in thisEntityMap) {
          // get the value of the fk field (which would be the id in the foreign object) 

          logger.info(`thisEntityKey: ${thisEntityKey}`)
          logger.info(`key: ${key}`)
          console.log(JSON.stringify(thisEntityMap[thisEntityKey], null, 2))
          let fkID = thisEntityMap[thisEntityKey][key]
          let fkObj = fkEntityMap[fkID] 

          //console.log(`fkid: ${fkID}, fkObj: ${fkObj}`)
          if (!fkObj[containerKey]) {
            fkObj[containerKey] = []
          }
          fkObj[containerKey].push(thisEntityMap[thisEntityKey])
        }
      })
    }
  }

  getIdMap(entity, id) {
    return this._idMaps[entity + '.' + id]
  }

  showStats() {
    let spec = this._spec
    for (let entity of Object.keys(spec)) {
      console.log( `Entity ${entity}:`)
      if (this[entity]) {
        for (let id of Object.keys(this[entity])) {
          console.log(' ' + JSON.stringify(this[entity][id]))
        }
      } else {
        console.log('  ... no records found ...')
      }
    }
    console.log('Id Maps:')
    console.log(JSON.stringify(this._idMaps, null, 2))
  }

  async init() {
    await this.fetchEntities(false)
    this.setupLinks()

    let updateFn = async () => {
      logger.info(`Instance ${this._instanceId}: Updating entities`)
      await this.fetchEntities(true)
    }

    if (this._options.refreshInterval) {
      logger.info('Initializing refresh')
      this._intervalTimer = setInterval(updateFn.bind(this), this._options.refreshInterval)
    }
  }

  destroy() {
    let spec = this._spec
    logger.info(`Destroying instance ${this._instanceId}`)
    for (let entity of Object.keys(spec)) {
      delete this[entity] 
    }
    clearInterval(this._intervalTimer)
  }
}

module.exports = DBGraph