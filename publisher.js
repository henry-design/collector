const amqp = require("amqplib/callback_api");
const dgram = require("dgram");
const PORT = 8011;
const HOST = "0.0.0.0";
const apiBase = require("./apiBase");

amqp.connect("amqp://localhost", function (error0, connection) {
  if (error0) {
    throw error0;
  }
  connection.createChannel(function (error1, channel) {
    if (error1) {
      throw error1;
    }
    function getDateTime() {
      var date = new Date();

      var hour = date.getHours();
      hour = (hour < 10 ? "0" : "") + hour;

      var min = date.getMinutes();
      min = (min < 10 ? "0" : "") + min;

      var sec = date.getSeconds();
      sec = (sec < 10 ? "0" : "") + sec;

      var year = date.getFullYear();

      var month = date.getMonth() + 1;
      month = (month < 10 ? "0" : "") + month;

      var day = date.getDate();
      day = (day < 10 ? "0" : "") + day;

      return year + month + day + hour + min + sec;
    }
    const YY = parseInt(getDateTime().slice(2, 4)).toString(16);
    const MM =
      (parseInt(getDateTime().slice(4, 6)).toString(16) < 10 ? "0" : "") +
      parseInt(getDateTime().slice(4, 6)).toString(16);
    const DD =
      (parseInt(getDateTime().slice(6, 8)).toString(16) < 10 ? "0" : "") +
      parseInt(getDateTime().slice(6, 8)).toString(16);
    const HH =
      (parseInt(getDateTime().slice(8, 10)).toString(16) < 10 ? "0" : "") +
      parseInt(getDateTime().slice(8, 10)).toString(16);
    const MI =
      (parseInt(getDateTime().slice(10, 12)).toString(16) < 10 ? "0" : "") +
      parseInt(getDateTime().slice(10, 12)).toString(16);
    const SS =
      (parseInt(getDateTime().slice(12, 14)).toString(16) < 10 ? "0" : "") +
      parseInt(getDateTime().slice(12, 14)).toString(16);

    const clock = YY + MM + DD + HH + MI + SS;
    const queueOne = "loginframe";
    const queueTwo = "dataframe";
    const queueThree = "timingCommand";

    const server = [];
    for (let i = 0; i < 1; i++) {
      server[i] = dgram.createSocket("udp4");

      server[i].on(
        "listening",
        function () {
          const address = this.address();
          console.log(
            `udp Server  listening on address ${address.address}  ${address.port}`
          );
        }.bind(server[i])
      );

      server[i].on("close", function () {
        console.log("udp socket closed..");
      });

      server[i].on(
        "message",
         function (message, remote) {
          console.log(
            "\n\nData received from bulk meter: " +
              Buffer.from(message, "ascii").toString("hex") +
              " "
          );
          const messageData = Buffer.from(message, "ascii").toString("hex");
          const getLoginFrame = (l1, l2, l3, l4, l5, l6, l7, l8, l9, l10) => {
            return parseInt(
              l1 + l2 + l3 + l4 + l5 + l6 + l7 + l8 + l9 + l10,
              10
            )
              .toString(16)
              .slice(-2);
          };
          let loginFrameClientAddress = messageData.slice(8, 20);
          const loginCheck = getLoginFrame(
            0x00,
            0x09,
            Number(`0x${messageData.slice(8, 10)}`),
            Number(`0x${messageData.slice(10, 12)}`),
            Number(`0x${messageData.slice(12, 14)}`),
            Number(`0x${messageData.slice(14, 16)}`),
            Number(`0x${messageData.slice(16, 18)}`),
            Number(`0x${messageData.slice(18, 20)}`),
            0x01,
            0x00
          )
          console.log(loginCheck);
          if (messageData.slice(20, 22) == 01) {
             this.send(
              (loginFrameReply = new Buffer.from(
                `403A0009${loginFrameClientAddress}0100${loginCheck}0D0A`,
                "hex"
              ).toString("ascii")),
              remote.port,
              remote.address,
              function (err, bytes) {
                if (err) throw err;
                console.log(
                  `Login Frame Reply Sent: ${Buffer.from(
                    loginFrameReply,
                    "ascii"
                  ).toString("hex")} bytes: ${bytes} sent to ${
                    remote.address
                  }:${remote.port}`
                );
              }
            );
          } else if (messageData.slice(20, 22) == 08) {
            let dataFramePacketNo = messageData.slice(24, 26); //24,26
            const getDataFrame = (
              l1,
              l2,
              l3,
              l4,
              l5,
              l6,
              l7,
              l8,
              l9,
              l10,
              l11,
              l12
            ) => {
              return parseInt(
                l1 + l2 + l3 + l4 + l5 + l6 + l7 + l8 + l9 + l10 + l11 + l12,
                10
              )
                .toString(16)
                .slice(-2);
            };
            const check = getDataFrame(
              0x00,
              0x0b,
              Number(`0x${messageData.slice(8, 10)}`),
              Number(`0x${messageData.slice(10, 12)}`),
              Number(`0x${messageData.slice(12, 14)}`),
              Number(`0x${messageData.slice(14, 16)}`),
              Number(`0x${messageData.slice(16, 18)}`),
              Number(`0x${messageData.slice(18, 20)}`),
              0x08,
              0x01,
              Number(`0x${messageData.slice(24, 26)}`),
              0x00
            );
            console.log(check);
              this.send(
              (dataSent = new Buffer.from(`403A000B${loginFrameClientAddress}0801${dataFramePacketNo}00${check}0D0A`,"hex").toString("ascii")),
              remote.port,
              remote.address,
              function (err, bytes) {
                if (err) throw err;
                console.log(
                  `Data Frame Reply Sent: ${Buffer.from(
                    dataSent,
                    "ascii"
                  ).toString("hex")} bytes: ${bytes} sent to ${
                    remote.address
                  }:${remote.port}`
                );
              }
            );
            const getTiming = (
              l1,
              l2,
              l3,
              l4,
              l5,
              l6,
              l7,
              l8,
              l9,
              l10,
              l11,
              l12,
              l13,
              l14,
              l15,
              l16
              
            ) => {
              let x = parseInt(
                l1 +
                  l2 +
                  l3 +
                  l4 +
                  l5 +
                  l6 +
                  l7 +
                  l8 +
                  l9 +
                  l10 +
                  l11 +
                  l12 +
                  l13 +
                  l14 +
                  l15 +
                  l16,
                  
                10
              )
                .toString(16)
                .slice(-2);
              console.log(messageData.slice(messageData.length-4, messageData.length));
              return x;
            };
            const timing = getTiming(
              0x00,
              0x0f,
              Number(`0x${messageData.slice(8, 10)}`),
              Number(`0x${messageData.slice(10, 12)}`),
              Number(`0x${messageData.slice(12, 14)}`),
              Number(`0x${messageData.slice(14, 16)}`),
              Number(`0x${messageData.slice(16, 18)}`),
              Number(`0x${messageData.slice(18, 20)}`),
              0x09,
              Number(`0x${YY}`),
              Number(`0x${MM}`),
              Number(`0x${DD}`),
              Number(`0x${HH}`),
              Number(`0x${MI}`),
              Number(`0x${SS}`),              
              0x00
            )

              this.send(
              (timingSent = new Buffer.from(`403A000F${loginFrameClientAddress}09${clock}00${timing}0D0A`,"hex").toString("ascii")),
              remote.port,
              remote.address,
              function (err, bytes) {
                if (err) throw err;
                console.log(
                  `Timing Command Sent: ${Buffer.from(
                    timingSent,
                    "ascii"
                  ).toString("hex")} bytes: ${bytes} sent to ${
                    remote.address
                  }:${remote.port}`
                );
              }
            );
          } else if (messageData.slice(20, 22) == 09) {
            console.log(`Timing command: ${messageData}`);
            console.log("function code not 08 or 01");
          }
          const msg = messageData;
          if (msg.slice(20, 22) == 01) {
            channel.sendToQueue(queueOne, Buffer.from(msg));
          } else if (msg.slice(20, 22) == 08) {
            channel.sendToQueue(queueTwo, Buffer.from(msg));
          } else if (msg.slice(20, 22) == 09) {
            console.log(`Timing command: ${msg}`);
            channel.sendToQueue(queueThree, Buffer.from(msg));
          }
          // console.log(" message sent to  :", msg, "message length", msg.length);
        }.bind(server[i])
      );
      server[i].bind(PORT + i, HOST);
    }
  });
});
