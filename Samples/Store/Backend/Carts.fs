﻿module Backend.Carts

open Domain
open Domain.Cart
open Foldunk

type CartService(streamer : IEventStream<Cart.Folds.State, Cart.Events.Event>) =
    let load log = Handler.load Cart.Folds.fold Cart.Folds.initial Cart.streamName streamer log 

    member __.Run (log : Serilog.ILogger) (cartId : CartId) decide = async {
        let maxAttempts = 1
        let! syncState = load log cartId
        return! Handler.run log maxAttempts syncState decide }

    member __.Load (log : Serilog.ILogger) (cartId : CartId) = async {
        let! state = load log cartId
        return state.State }