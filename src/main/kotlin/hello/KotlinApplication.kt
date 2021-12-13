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
import kotlin.math.absoluteValue


sealed interface BotDecision
data class Action(val action: String): BotDecision
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
        return when {
            self.y < y ->
                ChangeState(TurnTo("N", this))
            self.y > y ->
                ChangeState(TurnTo("S", this))
            self.x < x ->
                ChangeState(TurnTo("E", this))
            self.x > x ->
                ChangeState(TurnTo("W", this))
            else ->
                ChangeState(nextState)
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

@SpringBootApplication
class KotlinApplication {
    private var currentState: BotState = FindOpponent

    @Bean
    fun routes() = router {
        GET {
            ServerResponse.ok().body(Mono.just("Let the battle begin!"))
        }

        POST("/**", accept(APPLICATION_JSON)) { request ->

            request.bodyToMono(ArenaUpdate::class.java).flatMap { arenaUpdate ->
                writeCommittedStream!!.send(arenaUpdate.arena)

                val action: String = when(val decission = currentState.decideStuff(arenaUpdate)) {
                    is Action -> decission.action
                    is ChangeState -> {
                        currentState = decission.newState
                        "T"
                    }
                }

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

    fun turnTo(playerState: PlayerState): Action {
        when(direction) {
            "N" -> {

            }
        }
        TODO()
    }
}
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
}
