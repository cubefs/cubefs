/*
 * Copyright (c) 2021 IBM Corp and others.
 *
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v2.0
 * and Eclipse Distribution License v1.0 which accompany this distribution.
 *
 * The Eclipse Public License is available at
 *    https://www.eclipse.org/legal/epl-2.0/
 * and the Eclipse Distribution License is available at
 *   http://www.eclipse.org/org/documents/edl-v10.php.
 *
 * Contributors:
 *    Seth Hoenig
 *    Allan Stockdill-Mander
 *    Mike Robertson
 */

package mqtt

type component string

// Component names for debug output
const (
	NET component = "[net]     "
	PNG component = "[pinger]  "
	CLI component = "[client]  "
	DEC component = "[decode]  "
	MES component = "[message] "
	STR component = "[store]   "
	MID component = "[msgids]  "
	TST component = "[test]    "
	STA component = "[state]   "
	ERR component = "[error]   "
	ROU component = "[router]  "
)
