package hello

import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication
import org.springframework.context.annotation.Bean
import org.springframework.http.MediaType.APPLICATION_JSON
import org.springframework.web.reactive.function.server.ServerResponse
import org.springframework.web.reactive.function.server.body
import org.springframework.web.reactive.function.server.router
import reactor.core.publisher.Mono
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
        TODO()
    }
}

data class GoToPlayer(var targetPlayer: PlayerState) : BotState {
    override fun decideStuff(update: ArenaUpdate): BotDecision {
        TODO("Not yet implemented")
    }

}

data class ThrowAt(val playerState: PlayerState) : BotState {
    override fun decideStuff(update: ArenaUpdate): BotDecision {
        return Action("T")
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
}

fun main(args: Array<String>) {
    runApplication<KotlinApplication>(*args)
}

data class Position(val x: Int, val y: Int)

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
    fun canIThrow(playerState: PlayerState): Boolean {
        TODO()
    }

}
data class Links(val self: Self)
data class Self(val href: String)
data class Arena(val dims: List<Int>, val state: Map<String, PlayerState>) {
    fun findNearestPlayer(x: Int, y: Int): PlayerState {
        return state.values.map {
            val xDiff = (x - it.x).absoluteValue
            val yDiff = (y - it.x).absoluteValue
            it to (xDiff + yDiff)
        }.minByOrNull { it.second }!!.first
    }
}
