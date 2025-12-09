package com.better.alarm.domain

import android.content.Context
import com.better.alarm.logger.Logger
import org.eclipse.paho.android.service.MqttAndroidClient
import org.eclipse.paho.client.mqttv3.*
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence

/**
 * Simple MQTT Manager for single alarm state communication.
 */
class MqttAlarmManager(
    private val context: Context,
    private val logger: Logger,
    private val brokerUrl: String = "tcp://192.168.50.216:1883"
    //private val brokerUrl: String = "tcp://172.20.10.5:1883"
) : IMqttManager {

    private var mqttClient: MqttAndroidClient? = null
    private var dismissCallback: (() -> Unit)? = null
    private val alarmStateTopic = "alarm/state"

    // Track connection state
    @Volatile
    private var connectionState = "NOT_STARTED"

    init {
        logger.debug { "MqttAlarmManager init - Thread: ${Thread.currentThread().name}" }
        connectToMqtt()
    }
    private fun connectToMqtt() {
        try {
            connectionState = "INITIALIZING"
            logger.debug { "=== Starting MQTT Connection ===" }
            logger.debug { "Broker URL: $brokerUrl" }

            val clientId = "AlarmApp_${System.currentTimeMillis()}"
            logger.debug { "Client ID: $clientId" }

            mqttClient = MqttAndroidClient(context, brokerUrl, clientId, MemoryPersistence())
            logger.debug { "MqttAndroidClient created successfully" }

            mqttClient?.setCallback(object : MqttCallback {
                override fun connectionLost(cause: Throwable?) {
                    connectionState = "LOST"
                    logger.warning { "MQTT connection lost: ${cause?.message}" }
                    reconnect()
                }

                override fun messageArrived(topic: String?, message: MqttMessage?) {
                    logger.debug { "MQTT message arrived on topic: $topic" }
                    handleIncomingMessage(topic, message)
                }

                override fun deliveryComplete(token: IMqttDeliveryToken?) {
                    logger.trace { "MQTT delivery complete" }
                }
            })

            logger.debug { "Callback set successfully" }

            val options = MqttConnectOptions().apply {
                isAutomaticReconnect = true
                isCleanSession = true
                connectionTimeout = 30
                keepAliveInterval = 60
            }

            connectionState = "CONNECTING"
            logger.debug { "Calling connect()..." }

            // Use blocking connect with a token
            val token = mqttClient?.connect(options, null, object : IMqttActionListener {
                override fun onSuccess(asyncActionToken: IMqttToken?) {
                    connectionState = "CONNECTED"
                    logger.debug { "!!! MQTT CONNECTED SUCCESSFULLY !!!" }
                }

                override fun onFailure(asyncActionToken: IMqttToken?, exception: Throwable?) {
                    connectionState = "FAILED"
                    logger.warning { "!!! MQTT CONNECTION FAILED !!!" }
                    logger.warning { "Exception: ${exception?.message}" }
                    exception?.printStackTrace()
                }
            })

            // Wait for connection to complete (blocking)
            try {
                logger.debug { "Waiting for connection..." }
                token?.waitForCompletion(10000)  // Wait up to 10 seconds
                logger.debug { "Connection complete. Connected: ${mqttClient?.isConnected}" }
            } catch (e: Exception) {
                logger.warning { "Connection wait failed: ${e.message}" }
            }

        } catch (e: Exception) {
            connectionState = "EXCEPTION"
            logger.warning { "Exception in connectToMqtt: ${e.message}" }
            e.printStackTrace()
        }
    }

    private fun reconnect() {
        if (mqttClient?.isConnected == false) {
            try {
                val options = MqttConnectOptions().apply {
                    isAutomaticReconnect = true
                    isCleanSession = true
                }
                mqttClient?.connect(options)
            } catch (e: Exception) {
                logger.warning { "Reconnection failed: ${e.message}" }
            }
        }
    }

    private fun handleIncomingMessage(topic: String?, message: MqttMessage?) {
        if (topic == null || message == null) return

        try {
            val payload = String(message.payload).trim()
            logger.debug { "Received MQTT message: '$payload' on $topic" }

            // Check if message is "0" (dismiss command)
            if (topic == alarmStateTopic && (payload == "WATCH" || payload == "IDLE")) {
                logger.debug { "Alarm dismiss command received" }
                dismissCallback?.invoke()
            }
        } catch (e: Exception) {
            logger.warning { "Error handling MQTT message: ${e.message}" }
        }
    }

    override fun publishAlarmFired() {
        logger.debug { "publishAlarmFired() called" }
        logger.debug { "Connection state: $connectionState" }
        logger.debug { "isConnected: ${mqttClient?.isConnected}" }

        if (mqttClient?.isConnected == true) {
            logger.debug { "Publishing ALARM state..." }
            publishState("ALARM")
        } else {
            logger.warning { "MQTT not connected (state: $connectionState), retrying..." }

            // Try synchronous reconnect
            reconnectBlocking()

            if (mqttClient?.isConnected == true) {
                logger.debug { "Reconnect successful, publishing..." }
                publishState("ALARM")
            } else {
                logger.warning { "Reconnect failed, cannot publish" }
            }
        }
    }

    private fun reconnectBlocking() {
        try {
            logger.debug { "Attempting blocking reconnection..." }
            val options = MqttConnectOptions().apply {
                isAutomaticReconnect = true
                isCleanSession = true
                connectionTimeout = 5
            }
            val token = mqttClient?.connect(options)
            token?.waitForCompletion(5000)
            logger.debug { "Reconnect attempt complete. Connected: ${mqttClient?.isConnected}" }
        } catch (e: Exception) {
            logger.warning { "Blocking reconnect failed: ${e.message}" }
        }
    }

    private fun retryPublish(state: String, maxRetries: Int, delayMs: Long = 500) {
        logger.debug { "Scheduling retry publish for state: $state" }

        Thread {
            var attempts = 0
            while (attempts < maxRetries && mqttClient?.isConnected != true) {
                Thread.sleep(delayMs)
                attempts++
                logger.debug { "Retry attempt $attempts/$maxRetries - Connected: ${mqttClient?.isConnected}" }
            }

            if (mqttClient?.isConnected == true) {
                logger.debug { "Retry successful, publishing now" }
                publishState(state)
            } else {
                logger.warning { "Retry failed after $attempts attempts. State: $connectionState" }
            }
        }.start()
    }

    override fun publishPreAlarmFired() {
        publishState("PREALARM")
    }

    override fun subscribeToCommands(onDismiss: () -> Unit) {
        dismissCallback = onDismiss
        subscribeToAlarmState()
    }

    private fun publishState(state: String) {
        try {
            logger.debug { "publishState() called with: $state" }

            if (mqttClient?.isConnected != true) {
                logger.warning { "publishState aborted - not connected" }
                return
            }

            val message = MqttMessage(state.toByteArray()).apply {
                qos = 1
                isRetained = false
            }

            logger.debug { "Publishing to topic: $alarmStateTopic" }

            mqttClient?.publish(alarmStateTopic, message, null, object : IMqttActionListener {
                override fun onSuccess(asyncActionToken: IMqttToken?) {
                    logger.debug { "!!! Published successfully: $state !!!" }
                }

                override fun onFailure(asyncActionToken: IMqttToken?, exception: Throwable?) {
                    logger.warning { "Publish failed: ${exception?.message}" }
                    exception?.printStackTrace()
                }
            })

        } catch (e: Exception) {
            logger.warning { "Exception in publishState: ${e.message}" }
            e.printStackTrace()
        }
    }


    private fun subscribeToAlarmState() {
        try {
            if (mqttClient?.isConnected == true) {
                mqttClient?.subscribe(alarmStateTopic, 1, null, object : IMqttActionListener {
                    override fun onSuccess(asyncActionToken: IMqttToken?) {
                        logger.debug { "Subscribed to alarm state topic" }
                    }

                    override fun onFailure(asyncActionToken: IMqttToken?, exception: Throwable?) {
                        logger.warning { "Failed to subscribe to alarm state: ${exception?.message}" }
                    }
                })
            } else {
                logger.warning { "MQTT not connected, will subscribe on connection" }
            }
        } catch (e: Exception) {
            logger.warning { "Error subscribing to alarm state: ${e.message}" }
        }
    }

    fun disconnect() {
        try {
            mqttClient?.disconnect()
            mqttClient?.close()
            dismissCallback = null
            logger.debug { "MQTT disconnected" }
        } catch (e: Exception) {
            logger.warning { "Error disconnecting MQTT: ${e.message}" }
        }
    }

    fun isConnected(): Boolean = mqttClient?.isConnected == true
}

/**
 * Interface for MQTT alarm management
 */
interface IMqttManager {
    fun publishAlarmFired()
    fun publishPreAlarmFired()
    fun subscribeToCommands(onDismiss: () -> Unit)
}
