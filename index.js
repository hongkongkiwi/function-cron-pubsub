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
const configStorageLastRunFile = process.env.STORAGE_LASTRUN_FILE || 'lastrun'
const configStorageNextRunsFile = process.env.STORAGE_NEXT_RUNS_JSON_FILE || 'next_runs.json'
const credsStorageService = process.env.CREDENTIALS_STORAGE_SERVICE ? path.join(KEYS_DIR,process.env.CREDENTIALS_STORAGE_SERVICE) : null
const credsPubSub = process.env.CREDENTIALS_PUBSUB ? path.join(KEYS_DIR,process.env.CREDENTIALS_PUBSUB) : null
const timezone = process.env.TZ || 'Asia/Tokyo'
const httpKey = process.env.HTTP_KEY || null
const minRerunSeconds = 58

if (isNull(httpKey) ||
    isNull(projectId) ||
    isNull(configStorageBucketName) ||
    isNull(configStorageCronFile) ||
    isNull(configStorageNextRunsFile) ||
    isNull(configStorageLastRunFile) ||
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
  const topicFullName = `projects/${process.env.GCP_PROJECT}/topics/${topicName}`
  const publisher = pubsub.topic(topicFullName).publisher()
  return publisher.publish(dataBuffer)
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
    initUnirest()
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
        throw new Error(`Invalid Status Code Response! ${response.code}`)
      }
    } catch (err) {
      throw err
    }
  } else if (item.action.type === 'pubsub') {
    initPubSub()
    try {
      if (!has(item.action, 'topic') || item.action.topic.length === 0) {
        throw new Error('No pubsub topic defined!')
      }
      if (!has(item.action, 'data') || item.action.data.length === 0) {
        throw new Error('When using pubsub, must not have blank data!')
      }
      const messageId = await publishMessage(item.action.topic, item.action.data)
      console.log('PubSub Message Sent To Topic:',item.action.topic)
    } catch (err) {
      throw err
    }
  } else {
    throw new Error('Unknown action type')
  }
}

const initUnirest = () => {
  unirest = unirest || require('unirest')
}

const initPubSub = () => {
  PubSub = PubSub || require('@google-cloud/pubsub')
  pubsub = pubsub || new PubSub({
    keyFilename: path.join(__dirname,'keys',process.env.CREDENTIALS_PUBSUB),
    autoRetry: true,
    maxRetries: 3
  })
}

const initStorage = () => {
  Storage = Storage || require('@google-cloud/storage')
  configBucket = configBucket || new Storage({
                        projectId: projectId,
                        keyFilename: credsStorageService
                    }).bucket(configStorageBucketName)
}

const seconds_from_date = (compDate) => {
  return Math.round(Math.abs((new Date() - compDate) / 1000),0)
}

const handleCron = async () => {
  parser = parser || require('cron-parser')

  const options = {
    currentDate: Date.now(),
    tz: timezone
  }

  let cronItems = await getConfig(configStorageCronFile, true)
  let lastRunTime = await getConfig(configStorageLastRunFile, false)
  if (lastRunTime && seconds_from_date(new Date(parseInt(lastRunTime))) < minRerunSeconds) {
    return
  }
  await saveConfig(configStorageLastRunFile, new Date().getTime(), false)
  if (isNull(cronItems)) {
    await saveConfig(configStorageCronFile, {}, true)
  }
  if (Object.keys(cronItems).length === 0) {
    throw new Error('No cron jobs in config file')
  }
  let nextRuns = await getConfig(configStorageNextRunsFile, true) || {}
  for (let item of cronItems) {
    if (!has(item,'id')) {
      console.error(`Invalid Cron Entry (no id): ${item}`)
      continue
    }
    const itemId = item.id
    if (!has(item,'interval')) {
      console.error(`Invalid Cron Entry (no interval): ${item}`)
      continue
    }
    if (has(item,"enabled") && isBoolean(item.enabled) && item.enabled == false) {
      console.log('Cron Entry is disabled, skipping.')
      continue
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
  return saveConfig(configStorageNextRunsFile, nextRuns, true)
}

const getConfig = async (configName, isJsonFile) => {
  initStorage()
  let config
  try {
    const rawData = await configBucket.file(configName).download()
    if (isJsonFile) {
      try {
        config = JSON.parse(rawData)
      } catch (e) {
        config = null
      }
    } else {
      config = rawData.toString()
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

const saveConfig = async (configName, configData, isJsonFile) => {
  initStorage()
  let dataToSave
  if (isJsonFile) {
    dataToSave = JSON.stringify(configData,null,1)
  } else {
    dataToSave = configData.toString()
  }
  return await configBucket.file(configName).save(dataToSave)
}

/*
*  @function http
*  @param {object} request object received from the caller
*  @param {object} response object created in response to the request
*/
exports.http = async (req, res) => {
  if (((has(req.headers,'key') || has(req.headers,'Key')) && (req.headers.key !== httpKey && req.headers.Key !== httpKey)) ||
      ((has(req.query, 'key') || has(req.query, 'Key')) && (req.query.key !== httpKey && req.query.Key !== httpKey))) {
    const passedKey = req.headers.Key ? req.headers.Key : req.query.Key
    console.log(req.headers.key)
    console.log((has(req.headers,'key') || has(req.headers,'Key')) && (req.headers.key !== httpKey && req.headers.Key !== httpKey))
    console.error("User Passed Wrong Key", passedKey)
    return res.status(401).end("Access Denied")
  }
  switch (req.method) {
    case 'GET':
      try {
        await handleCron()
        console.log('Cron Finished!')
        res.status(200).end("OK")
      } catch (err) {
        console.error(err)
        res.status(500).end("Internal Error")
      }
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
