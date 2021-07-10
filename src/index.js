const express = require('express')
// Required db config to pass through all middlewares
const db = require('../config/db')
const app = express()
const {PORT} = require('./constants')

// Middleware to parse json body
app.use(express.json())

// Main route passing db configuration as argument because THAT 'require' returns a function
app.use('/', require('./services')(db))

// Middleware in case no routes matching
app.use((_, __, next) => {
  next(new Error('Path not found'))
})

// Middleware to manage errors
app.use((error, _, res, __) => {
  res.status(400).json({
    status: false,
    message: error.message
  })
})

app.listen(3000, () => {
  console.info(`> listening at http://localhost:${PORT}`)
})