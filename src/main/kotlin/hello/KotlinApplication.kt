package hello

import com.google.api.core.ApiFuture
import com.google.cloud.ServiceOptions
import com.google.cloud.bigquery.storage.v1.*
import org.json.JSONArray
import org.json.JSONObject
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication
import org.springframework.context.annotation.Bean
import org.springframework.http.MediaType.APPLICATION_JSON
import org.springframework.web.reactive.function.server.ServerResponse
import org.springframework.web.reactive.function.server.body
import org.springframework.web.reactive.function.server.router
import reactor.core.publisher.Mono
import java.time.Instant
import java.util.logging.Logger
import kotlin.math.absoluteValue


sealed interface BotDecision
data class Action(val action: String): BotDecision {
    companion object {
        val forward = Action("F")
        val left = Action("L")
        val right = Action("R")
        val fire = Action("T")
    }
}
data class ChangeState(val newState: BotState): BotDecision

sealed interface BotState {
    fun decideStuff(update: ArenaUpdate): BotDecision
}

object FindOpponent : BotState {
    override fun decideStuff(update: ArenaUpdate): BotDecision {
        val me = update.myself
        val nearestPlayer = update.arena.findNearestPlayer(me.x, me.y)
        return ChangeState(
            GoTo(nearestPlayer.x, nearestPlayer.y+1,
                TurnTo("N",
                    ThrowAt(nearestPlayer.x, nearestPlayer.y))))
    }
}

data class GoTo(val x: Int, val y: Int, val nextState: BotState) : BotState {
    override fun decideStuff(update: ArenaUpdate): BotDecision {
        val self = update.myself
        val desiredFacing = when {
            self.y < y -> "S"
            self.y > y -> "N"
            self.x < x -> "E"
            self.x > x -> "W"
            else -> return ChangeState(nextState)
        }

        return if (desiredFacing != self.direction) {
            ChangeState(TurnTo(desiredFacing, this))
        } else {
            if (update.arena.isNotEmpty(self.nextField())) {
                Action.fire
            } else {
                Action.forward
            }
        }
    }
}

data class TurnTo(val desiredFacing: String, val nextState: BotState) : BotState {
    override fun decideStuff(update: ArenaUpdate): BotDecision {
        val self = update.myself
        return if (self.direction == desiredFacing) ChangeState(nextState)
        else return Action("L")
    }
}

data class ThrowAt(val x: Int, val y: Int) : BotState {
    override fun decideStuff(update: ArenaUpdate): BotDecision {
        return Action("T") // TODO
    }
}

data class SingleAction(val action: Action, val nextState: BotState) : BotState {
    private var actionDone = false
    override fun decideStuff(update: ArenaUpdate): BotDecision {
        if (actionDone) {
            return action
        } else {
            return ChangeState(nextState)
        }
    }

}

data class RunAway(var steps: Int = 5) : BotState {

    override fun decideStuff(update: ArenaUpdate): BotDecision {
        val arena = update.arena
        val self = update.myself
        if (steps <= 0) {
            return ChangeState(FindOpponent)
        }

        steps -= 1
        if (arena.isEmpty(self.up())) {
            return ChangeState(
                TurnTo("N",
                    SingleAction(Action.forward, this)))
        }

        if (arena.isEmpty(self.down())) {
            return ChangeState(
                TurnTo("S",
                    SingleAction(Action.forward, this)))
        }

        if (arena.isEmpty(self.left())) {
            return ChangeState(
                TurnTo("W",
                    SingleAction(Action.forward, this)))
        }

        if (arena.isEmpty(self.right())) {
            return ChangeState(
                TurnTo("E",
                    SingleAction(Action.forward, this)))
        }
        return Action.fire
    }

}

object JustThrow : BotState {
    override fun decideStuff(update: ArenaUpdate): BotDecision = Action.fire
}
@SpringBootApplication
class KotlinApplication {
    private var currentState: BotState = FindOpponent
    private var lastTarget = Point(0,0)
    val logger = Logger.getLogger("Bot")

    @Bean
    fun routes() = router {
        GET("/reset") {
            currentState = FindOpponent
            ServerResponse.ok().body(Mono.just("Ok, reset done!"))
        }

        GET("/throw") {
            currentState = JustThrow
            ServerResponse.ok().body(Mono.just("Ok, I'll just throw"))
        }

        GET("/") {
            ServerResponse.ok().body(Mono.just("Let the battle begin!"))
        }


        var runningAway = 0
        POST("/**", accept(APPLICATION_JSON)) { request ->

            request.bodyToMono(ArenaUpdate::class.java).flatMap { arenaUpdate ->
                writeCommittedStream!!.send(arenaUpdate.arena)


                logger.info("Current state: $currentState")
                logger.info("My info: ${arenaUpdate.myself}")

                val myself = arenaUpdate.myself
                if (runningAway <= 0 && myself.wasHit) {
                    runningAway = 5
                    currentState = RunAway()
                } else {
                    runningAway--
                }

                var action = "T"
                var counter = 0
                while(true) {
                    counter++
                    if (counter >= 100) {
                        logger.info("It seems that I've hang on the decission! I'll just throw!")
                        break
                    }
                    val decission = currentState.decideStuff(arenaUpdate)
                    if (decission is ChangeState) {
                        currentState = decission.newState
                    } else if (decission is Action) {
                        action = decission.action
                        break
                    }
                }

                logger.info("My decission: $action")

                ServerResponse.ok().body(Mono.just(action))
            }
        }
    }


    class WriteCommittedStream(projectId: String?, datasetName: String?, tableName: String?) {
        var jsonStreamWriter: JsonStreamWriter? = null
        fun send(arena: Arena): ApiFuture<AppendRowsResponse> {
            val now: Instant = Instant.now()
            val jsonArray = JSONArray()
            arena.state.forEach { (url: String?, playerState: PlayerState) ->
                val jsonObject = JSONObject()
                jsonObject.put("x", playerState.x)
                jsonObject.put("y", playerState.y)
                jsonObject.put("direction", playerState.direction)
                jsonObject.put("wasHit", playerState.wasHit)
                jsonObject.put("score", playerState.score)
                jsonObject.put("player", url)
                jsonObject.put("timestamp", now.getEpochSecond() * 1000 * 1000)
                jsonArray.put(jsonObject)
            }
            return jsonStreamWriter!!.append(jsonArray)
        }

        init {
            BigQueryWriteClient.create().use { client ->
                val stream: WriteStream = WriteStream.newBuilder().setType(WriteStream.Type.COMMITTED).build()
                val parentTable: TableName = TableName.of(projectId, datasetName, tableName)
                val createWriteStreamRequest: CreateWriteStreamRequest = CreateWriteStreamRequest.newBuilder()
                    .setParent(parentTable.toString())
                    .setWriteStream(stream)
                    .build()
                val writeStream: WriteStream = client.createWriteStream(createWriteStreamRequest)
                jsonStreamWriter =
                    JsonStreamWriter.newBuilder(writeStream.getName(), writeStream.getTableSchema()).build()
            }
        }
    }

    val projectId: String = ServiceOptions.getDefaultProjectId()
    val datasetName = "snowball"
    val tableName = "events"
    var writeCommittedStream: WriteCommittedStream? = WriteCommittedStream(projectId, datasetName, tableName)
}

fun main(args: Array<String>) {
    runApplication<KotlinApplication>(*args)
}

data class ArenaUpdate(val _links: Links, val arena: Arena) {
    val myself: PlayerState
        get() {
            val myId = _links.self.href
            return arena.state[myId]!!
        }

    fun findPlayer(id: String): PlayerState {
        TODO()
    }
}
data class PlayerState(val x: Int, val y: Int, val direction: String, val score: Int, val wasHit: Boolean) {
    fun canIReach(playerState: PlayerState): Boolean {
        return if (playerState.x == x) {
            (playerState.y - y).absoluteValue <= 3
        } else if (playerState.y == y) {
            (playerState.x - x).absoluteValue <= 3
        } else false
    }

    fun nextField(): Point {
        return when(direction) {
            "N" -> Point(x, y-1)
            "S" -> Point(x, y+1)
            "E" -> Point(x+1, y)
            "W" -> Point(x-1, y)
            else -> TODO("This should not happen")
        }
    }

    fun up() = Point(x, y-1)
    fun down() = Point(x, y+1)
    fun left() = Point(x-1, y)
    fun right() = Point(x+1, y)
}

data class Point(val x: Int, val y: Int)

data class Links(val self: Self)
data class Self(val href: String)
data class Arena(val dims: List<Int>, val state: Map<String, PlayerState>) {
    fun findNearestPlayer(x: Int, y: Int): PlayerState {
        return state.values
            .map {
                val xDiff = (x - it.x).absoluteValue
                val yDiff = (y - it.x).absoluteValue
                it to (xDiff + yDiff)
            }
            .filter { it.second > 0 } // do not find myself
            .minByOrNull { it.second }!!.first
    }

    fun isNotEmpty(point: Point): Boolean {
        return !isEmpty(point)
    }

    fun isEmpty(point: Point): Boolean {
        if (point.x < 0 || point.y < 0 || point.x >= dims[0] || point.y >= dims[1]) {
            return false
        }
        return state.values.none { it.x == point.x && it.y == point.y }
    }
}
