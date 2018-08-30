"use strict"

const fs = require('mz/fs')
const path = require('path')
const {each,has,isNull,isObject,isBoolean,isArray,isNumber,isString} = require('lodash')
let PubSub
let Storage
let pubsub
let parser
let keyFile
let crypto
let configBucket
let unirest

process.env.NODE_ENV = process.env.NODE_ENV || 'development'
if (process.env.NODE_ENV === 'development') {
  /** Used when running on simulator **/
  const yamlFile = path.join(__dirname, '.env.yaml')

  if (fs.existsSync(yamlFile)) {
    const yaml = require('js-yaml')
    try {
      const config = yaml.safeLoad(fs.readFileSync(yamlFile, 'utf8'))
      for (let varName in config) {
        process.env[varName] = config[varName]
      }
    } catch (e) {
      console.error(e)
      process.exit(1)
    }
    process.env.GCP_PROJECT = 'squashed-melon'
    process.env.HTTP_TRIGGER_ENDPOINT = `http://localhost:8010/${process.env.GCP_PROJECT}/asia-northeast1/${process.env.FUNCTION_NAME}`
  }
}

const KEYS_DIR = path.join(__dirname, 'keys')
const projectId = process.env.GCP_PROJECT || null
const configStorageBucketName = process.env.STORAGE_BUCKET_NAME || null
const configStorageCronFile = process.env.STORAGE_CRON_JSON_FILE || 'cron.json'
const configStorageNextRunsFile = process.env.STORAGE_NEXT_RUNS_JSON_FILE || 'next_runs.json'
const credsStorageService = process.env.CREDENTIALS_STORAGE_SERVICE ? path.join(KEYS_DIR,process.env.CREDENTIALS_STORAGE_SERVICE) : null
const credsPubSub = process.env.CREDENTIALS_PUBSUB ? path.join(KEYS_DIR,process.env.CREDENTIALS_PUBSUB) : null
const timezone = process.env.TZ || 'Asia/Tokyo'

if (isNull(projectId) ||
    isNull(configStorageBucketName) ||
    isNull(configStorageCronFile) ||
    isNull(configStorageNextRunsFile) ||
    isNull(credsStorageService) ||
    isNull(credsPubSub)) {
      throw new Error("Environment Variables not available!")
}

const publishMessage = async (topicName, data) => {
  if (isNull(data) || data.length === 0) {
    throw new Error('PubSub data cannot be blank!')
  }
  let dataBuffer
  if (Buffer.isBuffer(data)) {
    dataBuffer = data
  } else if (isObject(data) || isArray(data)) {
    dataBuffer = Buffer.from(JSON.stringify(data), 'utf8')
  } else if (isString(data)) {
    dataBuffer = Buffer.from(data, 'utf8')
  // } else if (isNumber(data)) {
  //   dataBuffer = buf.readUInt32BE(0)
  // } else if (isFloat(data)) {
  }
  return pubsub
    .topic(`projects/${process.env.GCP_PROJECT}/topics/${topicName}`)
    .publisher()
    .publish(dataBuffer)
}

const listAllTopics = async () => {
  // Lists all topics in the current project
  const topics = await pubsub
    .getTopics()
  topics.forEach(topic => console.log(topic.name))
  return topics
}

const createTopic = async (topicName) => {
  // Creates a new topic
  console.log('creating a topic')
  const results = await pubsub
    .createTopic(topicName)
  return results.Topic
}

const checkTopicExists = async (topicName) => {
  const data = await pubsub
    .topic(topicName)
    .exists()
  return data[0]
}

const makeTopics = (topics) => {
  let promises = []
  for (let topicName of topics) {
    console.log('Checking if topic exists',topicName)
    const promise = checkTopicExists(topicName).then((exists) => {
      if (!exists) {
        console.log('Creating Topic')
        return createTopic(topicName)
      }
    })
    promises.push(promise)
  }
  return promises
}

const hashString = (str) => {
  crypto = crypto || require('crypto')
  return crypto.createHash('sha1').update(str).digest("hex")
}

const runCron = async (item) => {
  console.log('Run CRON:', item)
  if (item.action.type === 'http') {
    unirest = unirest || require('unirest')
    const method = item.action.method ? item.action.method.toUpperCase() : 'GET'
    const headers = item.action.headers || {}
    const body = item.action.body || null
    const expectedStatusCode = item.action.expectedStatusCode || 200
    if (!has(item.action, 'url') || item.action.url.length === 0) {
        throw new Error('No http url defined!')
    }
    try {
      const response = await unirest(method, item.action.url, headers, body)
      if (response.code !== expectedStatusCode) {
        throw new Error('Invalid Status Code Response!')
      }
    } catch (err) {
      throw err
    }
  } else if (item.action.type === 'pubsub') {
    try {
      if (!has(item.action, 'topic') || item.action.topic.length === 0) {
        throw new Error('No pubsub topic defined!')
      }
      if (!has(item.action, 'data') || item.action.data.length === 0) {
        throw new Error('When using pubsub, must not have blank data!')
      }
      await publishMessage(item.action.topic, item.action.data)
      console.log('PubSub Message Sent To Topic:',item.action.topic)
    } catch (err) {
      throw err
    }
  }
}

const handleCron = async () => {
  PubSub = PubSub || require('@google-cloud/pubsub')
  pubsub = pubsub || new PubSub({
    keyFilename: path.join(__dirname,'keys',process.env.CREDENTIALS_PUBSUB)
  })
  parser = parser || require('cron-parser')

  const options = {
    currentDate: Date.now(),
    tz: 'Asia/Hong_Kong'
  }

  let cronItems = await getConfig('cron.json')
  if (isNull(cronItems)) {
    await saveConfig(configStorageCronFile, {})
  }
  if (Object.keys(cronItems).length === 0) {
    throw new Error('No cron jobs in config file')
  }
  let nextRuns = await getConfig(configStorageNextRunsFile) || {}
  for (let item of cronItems) {
    if (!has(item,'id')) {
      throw new Error(`Invalid Cron Entry (no id): ${item}`)
    }
    const itemId = item.id
    if (!has(item,'interval')) {
      throw new Error(`Invalid Cron Entry (no interval): ${item}`)
    }
    if (has(item,"enabled") && isBoolean(item.enabled) && item.enabled == false) {
      console.log('Cron Entry is disabled, skipping.')
      return
    }
    const itemHash = hashString(JSON.stringify(item))
    let nextRunDate
    // Get the next run date
    try {
      nextRunDate = parser.parseExpression(item.interval, options).next().toDate()
      // Check if we have a next item time
      if (!has(nextRuns, itemId) || !isObject(nextRuns[itemId])) {
        nextRuns[itemId] = {}
      }
      if (!has(nextRuns[itemId], "hash") ||
          nextRuns[itemId].hash.length === 0 ||
          nextRuns[itemId].hash !== itemHash ||
          !has(nextRuns[itemId], "nextDate") ||
          nextRuns[itemId].nextDate.length === 0) {
        // Invalid Cron Run File
        nextRuns[itemId].hash = itemHash
        nextRuns[itemId].nextDate = nextRunDate
        nextRuns[itemId].failureCount = 0
        console.log('Updated Next CRON Run Info')
      }
      // Check if it's time to run it
      if (Date.now() > new Date(nextRuns[itemId].nextDate)) {
        // Run the cron
        try {
          await runCron(item)
          nextRuns[itemId].failureCount = 0
        } catch (err) {
          console.error("Error running cron:", err.message)
          nextRuns[itemId].failureCount++
        }
        nextRuns[itemId].hash = itemHash
        nextRuns[itemId].nextDate = nextRunDate
      }
    } catch (err) {
      console.log('Error:', err.message)
      // If there is an error, lets delete this from next run
      if (has(nextRuns, itemId)) {
        delete nextRuns[itemId]
      }
    }
  }
  return saveConfig(configStorageNextRunsFile, nextRuns)
}

const getConfig = async (configName) => {
  Storage = Storage || require('@google-cloud/storage')
  configBucket = configBucket || new Storage({
                        projectId: projectId,
                        keyFilename: credsStorageService
                    }).bucket(configStorageBucketName)
  let config
  try {
    const rawData = await configBucket.file(configName).download()
    try {
      config = JSON.parse(rawData)
    } catch (e) {
      config = null
    }
  } catch (err) {
    if (err.code ===  404) {
      config = null
    } else {
      console.error(err)
    }
  }
  return config
}

const saveConfig = async (configName, configData) => {
  Storage = Storage || require('@google-cloud/storage')
  configBucket = configBucket || new Storage({
                        projectId: projectId,
                        keyFilename: credsStorageService
                    }).bucket(configStorageBucketName)
  return await configBucket.file(configName).save(JSON.stringify(configData,null,1))
}

const handleGet = async (req, res) => {
  try {
    await handleCron()
    console.log('Cron Finished!')
    res.end(200, "OK")
  } catch (err) {
    console.error(err)
    res.end(500, "Internal Error")
  }
}

/*
*  @function http
*  @param {object} request object received from the caller
*  @param {object} response object created in response to the request
*/
exports.http = async (req, res) => {
  switch (req.method) {
    case 'GET':
      await handleGet(req, res)
      break
    default:
      handleError(`Invalid Method ${req.method}`, res)
      break
  }
}

/*
*
*  @function eventHelloWorld
*  @param { Object } event read event from configured pubsub topic
*  @param { Function } callback function
*/
exports.cronEvent = async (event, callback) => {
  try {
    await handleCron()
    console.log('Cron Check Finished')
    callback()
  } catch (err) {
    callback(err)
  }
}
