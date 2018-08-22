const PubSub = require('@google-cloud/pubsub')
const fs = require('mz/fs')
const path = require('path')
let pubsub

const keyFile = path.join(__dirname,'keys',process.env.CREDENTIALS_PUBSUB)
const crontab_topic = process.env.CRONTAB_TOPIC || 'cron'

const publishMessage = async (topicName, dataBuffer) => {
  const messageId = await pubsub
    .topic(topicName)
    .publisher()
    .publish(dataBuffer)
  return messageId
}

const listAllTopics = async () => {
  // Lists all topics in the current project
  const topics = await pubsub
    .getTopics()
  topics.forEach(topic => console.log(topic.name));
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

(async () => {
  pubsub = pubsub || new PubSub({
    keyFilename: keyFile
  })

  const topics = [
    `${crontab_topic}_weekend-hour-1`,
    `${crontab_topic}_weekday-hour-1`,
    `${crontab_topic}_workday-hour-1`,
    `${crontab_topic}_everyday-hour-2`,
    `${crontab_topic}_everyday-hour-1`,
    `${crontab_topic}_everyday-minute-30`,
    `${crontab_topic}_everyday-minute-15`,
    `${crontab_topic}_everyday-minute-5`,
    `${crontab_topic}_everyday-minute-1`
  ]

  const promises = makeTopics(topics)

  Promise.all(promises).then(() => {
    console.log('all things done')
  }).catch((err) => {
    console.error('Error',err)
  })

})()
