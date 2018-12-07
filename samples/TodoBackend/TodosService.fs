namespace TodoBackend

// NB - these schemas reflect the actual storage formats and hence need to be versioned with care
[<AutoOpen>]
module Events =
    type Todo = { id: int }
    type Event =
        | Added                                 of Todo
        | Deleted                               of int
        | Cleared                               of unit
        | Compacted                             of Todo[]
        interface TypeShape.UnionContract.IUnionContract

module Folds =
    type State = Todo list
    let initial = []
    let evolve s e =
        match e with
        | Added item -> item :: s
        | Deleted id -> s |> List.filter (fun x -> x.id <> id)
        | Cleared () -> initial
        | Compacted items -> List.ofArray items
    let fold state = Seq.fold evolve state
    let isOrigin = function Events.Compacted _ -> true | _ -> false
    let snapshot = isOrigin, fun state -> Compacted (Array.ofList state)

type Command = Add of Todo | Delete of id: int | Clear

module Commands =
    let interpret c _state =
        match c with
        | Add item -> [Added item]
        | Delete id -> [Deleted id]
        | Clear -> [Cleared ()]

type Handler(log, stream, ?maxAttempts) =
    let inner = Equinox.Handler(Folds.fold, log, stream, maxAttempts = defaultArg maxAttempts 2)
    member __.Execute command : Async<unit> =
        inner.Decide <| fun ctx ->
            ctx.Execute (Commands.interpret command)
    member __.Query(projection : Folds.State -> 't) : Async<'t> =
        inner.Query projection

type Service(handlerLog, resolve) =
    let stream = Handler(handlerLog, resolve (Equinox.CatId("Todos", "1")))

    member __.List : Async<Todo seq> =
        stream.Query Seq.ofList

    member __.TryGet id =
        stream.Query (List.tryFind (fun x -> x.id = id))

    member __.Execute command : Async<unit> =
        stream.Execute command