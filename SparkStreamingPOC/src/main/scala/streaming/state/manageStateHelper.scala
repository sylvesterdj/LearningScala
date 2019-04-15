package streaming.state

import org.apache.spark.sql.streaming.GroupState


case class StateEvents(key:String=null,value:List[InputValue]=List())

trait manageStateHelper {

  var state :StateEvents = _

  def manageState(key: String, inputEvents: Iterator[InputValue], groupState: GroupState[StateEvents]): List[InputValue] = {
    println("Key ::" + key)
    println("inputEvents ::" + inputEvents.toList)
    //println("groupState ::" + groupState.)

    val inputEventList = inputEvents.toList

    state = groupState.getOption.getOrElse(StateEvents())


    inputEventList.map( event => updateState(state,event))
    groupState.update(state)
    // state = groupState.getOption.getOrElse(InputValue())

    //groupState.update(updateState(state,inputEventList))
    if(groupState.exists) groupState.get.value else List()
  }

  def updateState(stateData:StateEvents,inputEvent:InputValue): StateEvents ={
    state = state.copy(value = state.value:+inputEvent)
    println("State :: " + state)
    state
  }
}
