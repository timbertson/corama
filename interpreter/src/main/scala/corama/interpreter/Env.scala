package corama.interpreter

import corama.VarId

/**
	* An environment is one *instance* of a topology execution, mapping vars to values.
	* A node maps one environment to 0..n environments — `Each` gives
	* one, `Filter` gives zero or one, `Explode` gives n.
	*
	* Continuing execution on a different node does not require any waiting on the source.
	* The evaluation is handed off, and any acknowledgement is routed to the original
	* requestor, not any of the intermediate nodes which played a part.
	*/
type Env = Map[VarId, Any]
