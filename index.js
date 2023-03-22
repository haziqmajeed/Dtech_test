const amqp = require('amqplib');
const express = require("express");
const app = express();
const PORT = 3000;
const fs = require("fs");
var mysql = require('mysql');

app.use(express.json());  
app.listen(PORT, () => {
  console.log(`Server running on port ${PORT}`);
});

const connection = mysql.createConnection({
  host: 'localhost',
  user: 'root',
  password: 'password',
  database: 'DtechLogger'
});

connection.connect(function(err) {
  if (err) throw err;
  console.log("Database Connected Successfully");
});


app.post('/sendMessage' , async (req, res) => {
  const {originator, fileName, fileContentLines} = req.body
  await sendMessage(fileContentLines)
  const status = await receiveMessage(fileName)
  if (!status) {
    writeToDB(originator, fileName, "Succesfully Processed")
    let response = {
      "returnCode": 0,
      "fileName": fileName,
      "status":"Succesfully Processed"
    }
    fs.writeFile('./responseFile.txt', JSON.stringify(response) , (err) => {
      if (err) throw err;
    });
    res.send(response);
  } else {
    writeToDB(originator, fileName, "Unsuccesfully Processed")
    let response = {
      "returnCode": 1,
      "fileName": fileName,
      "status":"Unsuccesfully Processed"
    }
    fs.writeFile('./responseFile.txt', JSON.stringify(response) , (err) => {
      if (err) throw err;
    });
    res.send(response);
  }
 
})

async function sendMessage(messages) {
  try {
    const connection = await amqp.connect('amqp://localhost');
    const channel = await connection.createChannel();

    const queueName = 'inbound';

    await channel.assertQueue(queueName);
    messages.map(async (message)=>{
      await channel.sendToQueue(queueName, Buffer.from(message));
    })
    

   
    await channel.close();
    await connection.close();
  } catch (error) {
    console.error(error);
  }
}

async function writeToDB(originator, fileName, status) {
  try {
    connection.query(`INSERT INTO detail (originator , filename  , status) VALUES ("${originator}", "${fileName}", "${status}")`, (err, rows) => {
      if (err) throw err;

    });
  } catch (err) {
    console.log("Something went wrong", err)
  }

}

async function receiveMessage(datafile) {
  try {
    const connection = await amqp.connect('amqp://localhost');
    const channel = await connection.createChannel();

    const queueName = 'inbound';

    await channel.assertQueue(queueName);
    await channel.consume(queueName, (message) => {
      fs.appendFile(`./${datafile}`, message.content.toString() + "\n" , (err) => {
        if (err) throw err;
      });
      channel.ack(message);
    });

    await channel.close();
    await connection.close();
    return 0
  } catch (error) {
    console.error("err", error);
    return 1
  }
}