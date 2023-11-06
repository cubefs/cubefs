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
	"fmt"
	"io"
)

// PubrelPacket is an internal representation of the fields of the
// Pubrel MQTT packet
type PubrelPacket struct {
	FixedHeader
	MessageID uint16
}

func (pr *PubrelPacket) String() string {
	return fmt.Sprintf("%s MessageID: %d", pr.FixedHeader, pr.MessageID)
}

func (pr *PubrelPacket) Write(w io.Writer) error {
	var err error
	pr.FixedHeader.RemainingLength = 2
	packet := pr.FixedHeader.pack()
	packet.Write(encodeUint16(pr.MessageID))
	_, err = packet.WriteTo(w)

	return err
}

// Unpack decodes the details of a ControlPacket after the fixed
// header has been read
func (pr *PubrelPacket) Unpack(b io.Reader) error {
	var err error
	pr.MessageID, err = decodeUint16(b)

	return err
}

// Details returns a Details struct containing the Qos and
// MessageID of this ControlPacket
func (pr *PubrelPacket) Details() Details {
	return Details{Qos: pr.Qos, MessageID: pr.MessageID}
}
