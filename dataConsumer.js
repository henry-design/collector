const amqp = require("amqplib/callback_api");
const axios = require("axios");

// *! header section of message
const header = "403A";
// console.log(header);

// *! tail section of message
const tail = "0D0A";
// console.log(tail);

amqp.connect("amqp://localhost", function(error0, connection) {
    if (error0) {
        throw error0;
    }
    connection.createChannel(function(error1, channel) {
        if (error1) {
            throw error1;
        }
        const queue = "dataframe";
        channel.assertQueue(queue, {
            durable: true,
        });
        console.log(" [*] Waiting for messages in %s. To exit press CTRL+C", queue);

        channel.consume(
            queue,
            function(msg) {
                if (msg.length <= 0) {
                    console.log("no message in queue");
                } else {
                    // *! Data frame 1 (send)

                    // ** intial payload Data frame 1 (send) message
                    const intialPayloadDataFrameSend = Buffer.from(
                        msg.content
                    ).toString();
                    console.log(`\nDataFrame received: ${intialPayloadDataFrameSend}\n`);
                    // console.log(intialPayloadDataFrameSend, "intialPayloadDataFrameSend");
                    //console.log("dataframeLength", intialPayloadDataFrameSend.length);

                    // ** start bit
                    const startbitLDataFrameSend = header;
                    // console.log(startbitLDataFrameSend);

                    //**  Data section length L
                    const datasectionDataFrameSend = intialPayloadDataFrameSend.slice(
                        4,
                        8
                    );
                    const datasectionDataFrameSendAnalysis = parseInt(
                        datasectionDataFrameSend,
                        16
                    );
                    // console.log("datasectionDataFrameSend", datasectionDataFrameSend);
                    // console.log(
                    //   "datasectionDataFrameSendAnalysis",
                    //   datasectionDataFrameSendAnalysis
                    // );

                    // ** Client address A
                    const clientAddressDataFrameSend = intialPayloadDataFrameSend.slice(
                        8,
                        20
                    );
                    // console.log("clientAddressDataFrameSend", clientAddressDataFrameSend);

                    // ** Function code C
                    const functionCodeDataFrameSend = intialPayloadDataFrameSend.slice(
                        20,
                        22
                    );
                    let functionCodeDataFrameSendAnalysis;
                    if (functionCodeDataFrameSend === "01") {
                        functionCodeDataFrameSendAnalysis = "Client login";
                    } else if (functionCodeDataFrameSend === "08") {
                        functionCodeDataFrameSendAnalysis = "reporting of many meters";
                    } else if (functionCodeDataFrameSend === "09") {
                        functionCodeDataFrameSendAnalysis = "set/read client time";
                    }
                    // console.log("functionCodeDataFrameSend", functionCodeDataFrameSend);
                    // console.log(
                    //   "functionCodeDataFrameSendAnalysis",
                    //   functionCodeDataFrameSendAnalysis
                    // );

                    // ** Total number of frames
                    const totalNumberofFramesDataFrameSend =
                        intialPayloadDataFrameSend.slice(22, 24);
                    console.log(
                      "totalNumberofFramesDataFrameSend:",
                      totalNumberofFramesDataFrameSend
                    );

                    // ** Which frame
                    const whichframeDataFrameSend = intialPayloadDataFrameSend.slice(
                        24,
                        26
                    );
                     console.log("whichframeDataFrameSend:", whichframeDataFrameSend);

                    // ** Number of this frame table
                    const numberofThisFrameTableDataFrameSend =
                        intialPayloadDataFrameSend.slice(26, 28);
                    console.log(
                      "numberofThisFrameTableDataFrameSend:",
                      numberofThisFrameTableDataFrameSend
                     );

                    function parseDataFrame() {
                        let dataBag = [];
                        let cosmosDataBag = [];
                        // *! Data frame 1 (send)


                        //**  Data section length L
                        const datasectionDataFrameSend = intialPayloadDataFrameSend.slice(
                            4,
                            8
                        );
                        const datasectionDataFrameSendAnalysis = parseInt(
                            datasectionDataFrameSend,
                            16
                        );
                        // console.log("datasectionDataFrameSend", datasectionDataFrameSend);
                        // console.log(
                        //   "datasectionDataFrameSendAnalysis",
                        //   datasectionDataFrameSendAnalysis
                        // );

                        // ** Client address A
                        const clientAddressDataFrameSend = intialPayloadDataFrameSend.slice(
                            8,
                            20
                        );
                        // console.log("clientAddressDataFrameSend", clientAddressDataFrameSend);

                        // ** Function code C
                        const functionCodeDataFrameSend = intialPayloadDataFrameSend.slice(
                            20,
                            22
                        );
                        let functionCodeDataFrameSendAnalysis;
                        if (functionCodeDataFrameSend === "01") {
                            functionCodeDataFrameSendAnalysis = "Client login";
                        } else if (functionCodeDataFrameSend === "08") {
                            functionCodeDataFrameSendAnalysis = "reporting of many meters";
                        } else if (functionCodeDataFrameSend === "09") {
                            functionCodeDataFrameSendAnalysis = "set/read client time";
                        }
                        // console.log("functionCodeDataFrameSend", functionCodeDataFrameSend);
                        // console.log(
                        //   "functionCodeDataFrameSendAnalysis",
                        //   functionCodeDataFrameSendAnalysis
                        // );

                        // ** Total number of frames
                        const totalNumberofFramesDataFrameSend =
                            intialPayloadDataFrameSend.slice(22, 24);
                        console.log(
                          "totalNumberofFramesDataFrameSend:",
                          totalNumberofFramesDataFrameSend
                        );

                        // ** Which frame
                        const whichframeDataFrameSend = intialPayloadDataFrameSend.slice(
                            24,
                            26
                        );
                        console.log("whichframeDataFrameSend:", whichframeDataFrameSend);

                        //  Number of this frame table
                        const numberofThisFrameTableDataFrameSend =
                            intialPayloadDataFrameSend.slice(26, 28);
                        console.log(
                          "numberofThisFrameTableDataFrameSend:",
                          numberofThisFrameTableDataFrameSend
                        );
                        instantaneousFlow = intialPayloadDataFrameSend.slice(28, intialPayloadDataFrameSend.length - 6);
                        var frames = [];

                        let dataSplit = () => {
                            var start = 0;
                            var end = 80;
                            for (var i = 0; i < instantaneousFlow.length / 80; i++) {
                                frames.push(instantaneousFlow.slice(start, end));
                                var start = start + 80;
                                var end = start + 80;
                            }
                            return frames;
                        };
                        dataSplit();

                        function decodeTelemetry() {
                            for (var i = 0; i < frames.length; i++) {
                                let deviceTelemetry = {};
                                let deviceTelemetryCosmosDb = {};

                                // ** Station number
                                const stationNumberDataFrameSend = frames[i].slice(0, 2);
                                // console.log("stationNumberDataFrameSend:", stationNumberDataFrameSend);

                                //** Meter type
                                const meterTypeDataFrameSend = frames[i].slice(2, 4);
                                // let meterTypeDataFrameSendAnalysis;
                                // if (meterTypeFrameSend === "84") {
                                //   meterTypeDataFrameSendAnalysis =
                                //     "SCL-61D stainless steel, with pressure";
                                // }
                                // console.log("meterTypeDataFrameSend:", meterTypeDataFrameSend);
                                // console.log("meterTypeDataFrameSendAnalysis:", meterTypeDataFrameSendAnalysis);

                                // ** Alarm code
                                const alarmCodeDataFrameSend = frames[i].slice(4, 6);
                                let alarmCodeDataFrameSendChoice;
                                if (alarmCodeDataFrameSend === "01") {
                                    alarmCodeDataFrameSendChoice =
                                        "positive flow exceeds the upper limit";
                                } else if (alarmCodeDataFrameSend === "02") {
                                    alarmCodeDataFrameSendChoice =
                                        "positive flow exceeds the lower limit";
                                } else if (alarmCodeDataFrameSend === "04") {
                                    alarmCodeDataFrameSendChoice =
                                        "negative flow exceeds the upper limit";
                                } else if (alarmCodeDataFrameSend === "08") {
                                    alarmCodeDataFrameSendChoice = "negative flow exceeds lower limit";
                                } else if (alarmCodeDataFrameSend === "10") {
                                    alarmCodeDataFrameSendChoice = "pressure exceeds the upper limit";
                                } else if (alarmCodeDataFrameSend === "20") {
                                    alarmCodeDataFrameSendChoice = "pressure exceeds the lower limit";
                                } else {
                                    alarmCodeDataFrameSendChoice = "No alarm recorded";
                                }
                                // console.log("alarmCodeDataFrameSend:", alarmCodeDataFrameSend);

                                // ** collecting time
                                const collectingTimeDataFrameSend = frames[i].slice(6, 16);
                                const collectingTimeDataFrameSendAnalysis = `${collectingTimeDataFrameSend}`;

                                const finalTimeData = `20${collectingTimeDataFrameSendAnalysis.slice(
              0,
              2
            )}-${collectingTimeDataFrameSendAnalysis.slice(
              2,
              4
            )}-${collectingTimeDataFrameSendAnalysis.slice(
              4,
              6
            )}T${collectingTimeDataFrameSendAnalysis.slice(
              6,
              8
            )}:${collectingTimeDataFrameSendAnalysis.slice(8, 10)}`;
                                const formattedDate = new Date(finalTimeData);
                                // console.log("firtstSlice:", firtstSlice);
                                // console.log(
                                //   "collectingTimeDataFrameSendAnalysis:",
                                //   collectingTimeDataFrameSendAnalysis
                                // );
                                // console.log("collectingTimeDataFrameSend:", collectingTimeDataFrameSend);

                                //** Meter reading data is valid
                                const meterReadingDataIsValidDataFrameSend = frames[i].slice(16, 18);
                                let meterReadingDataIsValidDataFrameSendAnalysis;
                                if (meterReadingDataIsValidDataFrameSend === "26") {
                                    meterReadingDataIsValidDataFrameSendAnalysis =
                                        "Meter reading data is valid";
                                } else {
                                    meterReadingDataIsValidDataFrameSendAnalysis =
                                        "Meter reading is invalid";
                                }
                                // console.log(
                                //   "meterReadingDataIsValidDataFrameSend:",
                                //   meterReadingDataIsValidDataFrameSend
                                // );
                                // console.log(
                                //   "meterReadingDataIsValidDataFrameSendAnalysis:",
                                //   meterReadingDataIsValidDataFrameSendAnalysis
                                // );

                                // ** The instantaneous flow is negative
                                const instantaneousFlowIsNegativeDataFrameSend = frames[i].slice(18, 20);
                                let instantaneousFlowIsNegativeDataFrameSendAnalysis;
                                if (instantaneousFlowIsNegativeDataFrameSend === "00") {
                                    instantaneousFlowIsNegativeDataFrameSendAnalysis =
                                        "The instantaneous flow is positive";
                                } else if (instantaneousFlowIsNegativeDataFrameSend === "0A") {
                                    instantaneousFlowIsNegativeDataFrameSendAnalysis =
                                        "The instantaneous flow is negative";
                                }
                                // console.log(
                                //   "instantaneousFlowIsNegativeDataFrameSend:",
                                //   instantaneousFlowIsNegativeDataFrameSend
                                // );
                                // console.log(
                                //   "instantaneousFlowIsNegativeDataFrameSendAnalysis:",
                                //   instantaneousFlowIsNegativeDataFrameSendAnalysis
                                // );

                                // ** Instantaneous flow
                                const instantaneousFlowDataFrameSend = frames[i].slice(20, 28);
                                const instantaneousFlowDataFrameSendAnalysis =
                                    instantaneousFlowDataFrameSend / 1000;
                                let instantaneousFlowDataFrameSendAnalysisFinal;
                                if (instantaneousFlowIsNegativeDataFrameSend === "00") {
                                    instantaneousFlowDataFrameSendAnalysisFinal =
                                        instantaneousFlowDataFrameSendAnalysis;
                                } else if (instantaneousFlowIsNegativeDataFrameSend === "0A") {
                                    instantaneousFlowDataFrameSendAnalysisFinal = `-${instantaneousFlowDataFrameSendAnalysis}`;
                                }
                                // console.log("instantaneousFlowDataFrameSend:", instantaneousFlowDataFrameSend);
                                // console.log(
                                //   "instantaneousFlowDataFrameSendAnalysis:",
                                //   instantaneousFlowDataFrameSendAnalysis
                                // );
                                // console.log(
                                //   "instantaneousFlowDataFrameSendAnalysisFinal:",
                                //   instantaneousFlowDataFrameSendAnalysisFinal
                                // );

                                // ** Negative cumulative flow
                                const negativeCummilativeFlowDataFrameSend = frames[i].slice(28, 36);
                                const negativeCummilativeFlowDataFrameSendAnalysis =
                                    negativeCummilativeFlowDataFrameSend / 10;
                                // console.log(
                                //   "negativeCummilativeFlowDataFrameSend:",
                                //   negativeCummilativeFlowDataFrameSend
                                // );
                                // console.log(
                                //   "negativeCummilativeFlowDataFrameSendAnalysis:",
                                //   negativeCummilativeFlowDataFrameSendAnalysis
                                // );

                                // ** Negative cumulative running time
                                const negativeCummilativeRunningTimeDataFrameSend =
                                    frames[i].slice(36, 44);
                                const negativeCummilativeRunningTimeDataFrameSendAnalysis =
                                    negativeCummilativeRunningTimeDataFrameSend * 1;
                                // console.log(
                                //   "negativeCummilativeRunningTimeDataFrameSend:",
                                //   negativeCummilativeRunningTimeDataFrameSend
                                // );
                                // console.log(
                                //   "negativeCummilativeRunningTimeDataFrameSendAnalysis:",
                                //   negativeCummilativeRunningTimeDataFrameSendAnalysis
                                // );

                                // ** Positive cumulative flow
                                const positiveCumulativeFlowDataFrameSend =
                                    frames[i].slice(44, 52);
                                const positiveCumulativeFlowDataFrameSendAnalysis =
                                    (positiveCumulativeFlowDataFrameSend * 1) / 10;
                                // console.log(
                                //   "positiveCumulativeFlowDataFrameSend:",
                                //   positiveCumulativeFlowDataFrameSend
                                // );
                                // console.log(
                                //   "positiveCumulativeFlowDataFrameSendAnalysis:",
                                //   positiveCumulativeFlowDataFrameSendAnalysis
                                // );

                                // ** Positive cumulative running time
                                const positiveCumulativeRunningTimeDataFrameSend =
                                    frames[i].slice(52, 60);
                                const positiveCumulativeRunningTimeDataFrameSendAnalysis =
                                    positiveCumulativeRunningTimeDataFrameSend * 1;
                                // console.log(
                                //   "positiveCumulativeRunningTimeDataFrameSend:",
                                //   positiveCumulativeRunningTimeDataFrameSend
                                // );
                                // console.log(
                                //   "positiveCumulativeRunningTimeDataFrameSendAnalysis:",
                                //   positiveCumulativeRunningTimeDataFrameSendAnalysis
                                // );

                                //** Water temperature
                                const waterTemperatureDataFrameSend =
                                    frames[i].slice(60, 68);
                                const waterTemperatureDataFrameSendAnalysis =
                                    (waterTemperatureDataFrameSend * 1) / 100;
                                // console.log("waterTemperatureDataFrameSend:", waterTemperatureDataFrameSend);
                                // console.log(
                                //   "waterTemperatureDataFrameSendAnalysis:",
                                //   waterTemperatureDataFrameSendAnalysis
                                // );

                                // ** pressure
                                const pressureDataFrameSend = frames[i].slice(68, 76);
                                const pressureDataFrameSendAnalysis = pressureDataFrameSend / 1000;
                                // console.log("pressureDataFrameSend:", pressureDataFrameSend);
                                // console.log("pressureDataFrameSendAnalysis:", pressureDataFrameSendAnalysis);

                                // ** Diagnostic code
                                const diagnosticCodeDataFrameSend = frames[i].slice(76, 78);
                                let diagnosticCodeDataFrameSendChoice;
                                if (diagnosticCodeDataFrameSend === "01") {
                                    diagnosticCodeDataFrameSendChoice =
                                        "The battery voltage is lower than 3.37V, the battery needs to be replaced";
                                } else if (diagnosticCodeDataFrameSend === "02") {
                                    diagnosticCodeDataFrameSendChoice =
                                        "Empty tube or no measurement signal caused by transducer failure";
                                } else if (diagnosticCodeDataFrameSend === "03") {
                                    diagnosticCodeDataFrameSendChoice =
                                        "Code 01 and Code 02 occur at the same time";
                                } else if (diagnosticCodeDataFrameSend === "04") {
                                    diagnosticCodeDataFrameSendChoice =
                                        "The battery voltage is lower than 3.3V, the battery must be replaced";
                                } else if (diagnosticCodeDataFrameSend === "05") {
                                    diagnosticCodeDataFrameSendChoice =
                                        "Communication failure between sensor and transducer, no communication";
                                } else if (diagnosticCodeDataFrameSend === "06") {
                                    diagnosticCodeDataFrameSendChoice = "E2PROM is damaged";
                                } else if (diagnosticCodeDataFrameSend === "10") {
                                    diagnosticCodeDataFrameSendChoice =
                                        "Water supply temperature sensor fault (short circuit, open circuit) or water supply temperature below 2 ℃";
                                } else if (diagnosticCodeDataFrameSend === "20") {
                                    diagnosticCodeDataFrameSendChoice =
                                        "Water supply temperature exceeds 150℃";
                                } else if (diagnosticCodeDataFrameSend === "40") {
                                    diagnosticCodeDataFrameSendChoice =
                                        "Return water temperature sensor fault (short circuit, open circuit) or return water temperature below 2 ℃";
                                } else if (diagnosticCodeDataFrameSend === "80") {
                                    diagnosticCodeDataFrameSendChoice =
                                        "The return water temperature exceeds 150℃";
                                } else {
                                    diagnosticCodeDataFrameSendChoice = "No diagonistic data";
                                }
                                // console.log("diagnosticCodeDataFrameSend:", diagnosticCodeDataFrameSend);
                                // console.log(
                                //   "diagnosticCodeDataFrameSendChoice:",
                                //   diagnosticCodeDataFrameSendChoice
                                // );

                                // ** Data check
                                const dataCheckDataFrameSend = frames[i].slice(78, 80);
                                // console.log("dataCheckDataFrameSend:", dataCheckDataFrameSend);
                                //todo: more configuration needed

                                // ** Check bit CS

                                // console.log("checkBitCsDataFrameSend:", checkBitCsDataFrameSend);

                                // ** Stop bit

                                // console.log("stopBitDataFrameSend:", stopBitDataFrameSend);
                                deviceTelemetry = {
                                    //*? data frame data

                                    clientAddressData: `${clientAddressDataFrameSend}`,
                                    alarmCodeData: `${alarmCodeDataFrameSendChoice}`,
                                    collectingTimeData: `${finalTimeData}`,
                                    negativeCummilativeFlowData: negativeCummilativeFlowDataFrameSendAnalysis,
                                    negativeCummilativeRunningTimeData: negativeCummilativeRunningTimeDataFrameSendAnalysis,
                                    positiveCumulativeFlowData: positiveCumulativeFlowDataFrameSendAnalysis,
                                    waterTemperatureData: waterTemperatureDataFrameSendAnalysis,
                                    pressureData: pressureDataFrameSendAnalysis,
                                    diagnosticCodeData: `${diagnosticCodeDataFrameSendChoice}`,
                                };
                                deviceTelemetryCosmosDb = {
                                    //*? data frame data
                                    payload: `${intialPayloadDataFrameSend}`,
                                    clientAddressData: `${clientAddressDataFrameSend}`,
                                    alarmCodeData: `${alarmCodeDataFrameSendChoice}`,
                                    collectingTimeData: `${formattedDate.toISOString()}`,
                                    negativeCummilativeFlowData: negativeCummilativeFlowDataFrameSendAnalysis,
                                    negativeCummilativeRunningTimeData: negativeCummilativeRunningTimeDataFrameSendAnalysis,
                                    positiveCumulativeFlowData: positiveCumulativeFlowDataFrameSendAnalysis,
                                    waterTemperatureData: waterTemperatureDataFrameSendAnalysis,
                                    pressureData: pressureDataFrameSendAnalysis,
                                    diagnosticCodeData: `${diagnosticCodeDataFrameSendChoice}`,
                                };
                                dataBag.push(deviceTelemetry)
                                cosmosDataBag.push(deviceTelemetryCosmosDb)
                            }


                        }
                        decodeTelemetry();
                        const checkBitCsDataFrameSend = intialPayloadDataFrameSend.slice(
                            108,
                            110
                        );
                        // console.log(dataBag);
                        return dataBag
                    }


                    let message = parseDataFrame();
                   

                    for (var i = 0; i < message.length; i++) {
                        const deviceTelemetryDataProduction = JSON.stringify(message[i]);
                        
                        // console.log(" [x] Received %s", msg.content.toString());
                        const sharesAccessSignature =
                            "SharedAccessSignature sr=testBulkMeterIotHub.azure-devices.net%2Fdevices%2FbulkMeter&sig=vv58zgYeIeboLncb%2FC41UKj6ud36qn1mG6EV6ogNuUw%3D&se=1628305176";
                        if (deviceTelemetryDataProduction) {
                            //**  post to http endpoint
                            axios
                                .post(
                                    "https://gosoftcoreapi.azurewebsites.net/api/Admin/ZonalMeterTelemetry",
                                    deviceTelemetryDataProduction, {
                                        headers: {
                                            "Content-Type": "application/json",
                                        },
                                    }
                                )
                                .then(
                                    (response) => {
                                        // console.log(
                                        //     "responseDataFrame after post request",
                                        //     response.data
                                        // );
                                        console.log(
                                            `production data (data frame): ${deviceTelemetryDataProduction}`
                                        );
                                        console.log(`responseData Axios: ${response.status}\n`);
                                        
                                    },
                                    (error) => {
                                        console.log("errorData Axios", error);
                                    }
                                );
                        } else {
                            // axios
                            //   .post(
                            //     "https://testBulkMeterIotHub.azure-devices.net/devices/bulkMeter/messages/events?api-version=2020-03-13",
                            //     {
                            //       device: "bulkMeter",
                            //       data: deviceTelemetryCosmosDBProduction,
                            //     },
                            //     {
                            //       headers: {
                            //         Authorization: `${sharesAccessSignature}`,
                            //       },
                            //     }
                            //   )
                            //   .then(
                            //     (response) => {
                            //       console.log("responseData Axios", response.status);
                            //     },
                            //     (error) => {
                            //       console.log("errorData Axios", error);
                            //     }
                            //   );
                            console.log("not sending data frame to  cosmos");

                        }
                    }
                }
            }, {
                noAck: true,
            }
        );
    });
});