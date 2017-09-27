﻿module Foldunk.InMemoryStore.Integration.InMemoryStoreIntegration

open Swensen.Unquote

let inline createMemStore () =
    Foldunk.Stores.InMemoryStore.InMemoryStreamStore()
let inline createMemStream<'state,'event> store streamName : Foldunk.IStream<'state,'event> =
    Foldunk.Stores.InMemoryStore.InMemoryStream(store, streamName) :> _

let createServiceWithInMemoryStore () =
    let store = createMemStore()
    Backend.Cart.Service(fun _codec -> createMemStream store)

#nowarn "1182" // From hereon in, we may have some 'unused' privates (the tests)

type Tests(testOutputHelper) =
    let testOutput = TestOutputAdapter testOutputHelper
    let createLog () = createLogger (testOutput.Subscribe >> ignore)

    [<AutoData>]
    let ``Basic tracer bullet, sending a command and verifying the folded result directly and via a reload``
            cartId1 cartId2 ((_,skuId,quantity) as args) = Async.RunSynchronously <| async {
        let log, service = createLog (), createServiceWithInMemoryStore ()
        let decide (ctx: Foldunk.DecisionContext<_,_>) = async {
            Domain.Cart.Commands.AddItem args |> Domain.Cart.Commands.interpret |> ctx.Execute
            return ctx.Complete ctx.State }

        // Act: Run the decision twice...
        let actTrappingStateAsSaved cartId =
            service.Decide log cartId decide
        let actLoadingStateSeparately cartId = async {
            let! _ = service.Decide log cartId decide
            return! service.Load log cartId }
        let! expected = cartId1 |> actTrappingStateAsSaved
        let! actual = cartId2 |> actLoadingStateSeparately

        // Assert 1. Despite being on different streams (and being in-memory vs re-loaded) we expect the same outcome
        test <@ expected = actual @>

        // Assert 2. Verify that the Command got correctly reflected in the state, with no extraneous effects
        let verifyFoldedStateReflectsCommand = function
            | { Domain.Cart.Folds.State.items = [ item ] } ->
                let expectedItem : Domain.Cart.Folds.ItemInfo = { skuId = skuId; quantity = quantity; returnsWaived = false }
                test <@ expectedItem = item @>
            | x -> x |> failwithf "Expected to find item, got %A"
        verifyFoldedStateReflectsCommand expected
        verifyFoldedStateReflectsCommand actual
    }