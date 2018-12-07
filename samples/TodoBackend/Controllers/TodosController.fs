namespace TodoBackend.Controllers

open Microsoft.AspNetCore.Http.Extensions // GetEncodedUrl
open Microsoft.AspNetCore.Mvc
open TodoBackend

type TodoView = { id: int; url: string }

// Stolen from https://github.com/ChristianAlexander/dotnetcore-todo-webapi/blob/master/src/TodoWebApi/Controllers/TodosController.cs
// even more stolen from https://github.com/joeaudette/playground/blob/master/spa-stack/src/FSharp.WebLib/Controllers.fs
[<Route "[controller]">]
type TodosController(service: Service) =
    inherit Controller()

    member private __.WithUri(x : Todo, ?id) : TodoView =
        let self = __.Request.GetEncodedUrl()
        { id = x.id; url = match id with None -> self | Some id -> sprintf "%s/%O" self id }

    [<HttpGet>]
    member __.Get() : Async<TodoView seq> = async {
        let! xs = service.List
        return seq { for x in xs -> __.WithUri(x, x.id) }
    }

    [<HttpGet"{id}">]
    member __.Get id : Async<IActionResult> = async {
        let! x = service.TryGet id
        return match x with None -> __.NotFound() :> _ | Some x -> ObjectResult(__.WithUri x) :> _
    }

    [<HttpPost>]
    member __.Post([<FromBody>]todo) : Async<TodoView> = async {
        do! service.Execute <| Add todo
        return __.WithUri todo
    }

    [<HttpDelete "{id}">]
    member __.Delete id : Async<unit> =
        service.Execute <| Delete id

    [<HttpDelete>]
    member __.Clear() : Async<unit> =
        service.Execute <| Clear