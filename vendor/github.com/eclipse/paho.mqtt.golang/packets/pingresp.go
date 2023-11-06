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
 *    Allan Stockdill-Mander
 */

package packets

import (
	"io"
)

// PingrespPacket is an internal representation of the fields of the
// Pingresp MQTT packet
type PingrespPacket struct {
	FixedHeader
}

func (pr *PingrespPacket) String() string {
	return pr.FixedHeader.String()
}

func (pr *PingrespPacket) Write(w io.Writer) error {
	packet := pr.FixedHeader.pack()
	_, err := packet.WriteTo(w)

	return err
}

// Unpack decodes the details of a ControlPacket after the fixed
// header has been read
func (pr *PingrespPacket) Unpack(b io.Reader) error {
	return nil
}

// Details returns a Details struct containing the Qos and
// MessageID of this ControlPacket
func (pr *PingrespPacket) Details() Details {
	return Details{Qos: 0, MessageID: 0}
}
