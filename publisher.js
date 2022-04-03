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
    const queueOne = "loginframe";
    const queueTwo = "dataframe";
    const server = [];
    for (let i = 0; i < 1; i++) {
      server[i] = dgram.createSocket("udp4");

      server[i].on(
        "listening",
        function () {
          const address = this.address();
          console.log(
            "udp Server listening on " + address.address + ":" + address.port
          );
        }.bind(server[i])
      );

      server[i].on("close", function () {
        console.log("udp socket closed..");
      });

      server[i].on(
        "message",
        async function (message, remote) {
          console.log(
            "Data received from bulk meter: " +
              Buffer.from(message, "ascii").toString("hex")
          );
          const messageData = Buffer.from(message, "ascii").toString("hex");
          const getLoginFrame = (l1, l2, l3, l4, l5, l6, l7, l8, l9, l10) => {
            return parseInt(
              l1 + l2 + l3 + l4 + l5 + l6 + l7 + l8 + l9 + l10,
              10
            )
              .toString(16)
              .slice(1, 3);
          };
          let loginFrameClientAddress = messageData.slice(8, 20);
          if (messageData.slice(20, 22) == 01) {
            await this.send(
              (loginFrameReply = new Buffer.from(
                `403A0009${loginFrameClientAddress}0100${getLoginFrame(
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
                )}0D0A`,
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
            let dataFramePacketNo = messageData.slice(24, 26);
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
                .slice(1, 3);
            };
            await this.send(
              (dataSent = new Buffer.from(
                `403A000B${loginFrameClientAddress}0801${dataFramePacketNo}00${getDataFrame(
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
                )}0D0A`,
                "hex"
              ).toString("ascii")),
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
          } else {
            console.log("function code not 08 or 01");
          }
          const msg = messageData;
                if (msg.slice(20, 22) == 01) {
                  channel.sendToQueue(queueOne, Buffer.from(msg));
                } else if (msg.slice(20, 22) == 08) {
                  channel.sendToQueue(queueTwo, Buffer.from(msg));
                } else {
                  console.log("msg slice not 01 or 08");
                }
          // console.log(" message sent to  :", msg, "message length", msg.length);
        }.bind(server[i])
      );
      server[i].bind(PORT + i, HOST);
    }
  });
});
