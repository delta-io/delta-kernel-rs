# Roadmap

This document represents a draft of a potential roadmap for delta-kernel-rs

The whole premise of delta-kernel is to address gaps identified in the
proliferation of APIs and custom implementations of the Delta protocol. This
should learn from those and provide a simpler or easier way for _all_ those
integrations to make following Delta Lake easier.


## Data and AI Summit

Goals:

* Get a topic branch in `delta-rs` functional with a minimal use-case,
  replacing some key functionality.
  * (?) Perhaps a CDC or Deletion vector use-case, to showcase the benefit.
    Denny is more interested in psuedo-code level
* Defined feature strategy for library vs user API surfaces
  * Singular user API implemented, perhaps arrow2 to demonstrate the
    hot-swappability versus strict delta-rs
* Basic scaffolding of traits defined with issues created in repository for
  contributors to participate
* Strong CI pipeline for getting new contributors on-board.
* **EVERYTHING** that exists gratuitously documented in rustdoc with doc tests
* Show how using delta-kernel-rs would be simpler for an API implementor
     * Selling this idea/API to future delta integrations

Demo ideas (order of priority):

* wasm in the browser for delta integration.
* Bootstrap simple Ruby client
* Bootstrap simple Node client
