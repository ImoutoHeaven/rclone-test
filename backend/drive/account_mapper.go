package drive

import (
	"strings"

	"github.com/rclone/rclone/fs/config"
	"github.com/rclone/rclone/fs/config/configmap"
)

type accountMapper struct {
	store *accountStore
	base  configmap.Getter
	index int
}

func (m accountMapper) Get(key string) (value string, ok bool) {
	if m.store != nil {
		switch key {
		case config.ConfigToken:
			if token, found := m.store.accountToken(m.index); found {
				return token, true
			}
		case config.ConfigClientID:
			if clientID, found := m.store.accountClientID(m.index); found {
				return clientID, true
			}
		case config.ConfigClientSecret:
			if clientSecret, found := m.store.accountClientSecret(m.index); found {
				return clientSecret, true
			}
		}
	}

	if m.base == nil {
		return "", false
	}
	return m.base.Get(key)
}

func (m accountMapper) Set(key, value string) {
	if m.store == nil {
		return
	}

	switch key {
	case config.ConfigToken:
		m.store.setAccountToken(m.index, strings.TrimSpace(value))
	case config.ConfigClientID:
		m.store.setAccountClientID(m.index, strings.TrimSpace(value))
	case config.ConfigClientSecret:
		m.store.setAccountClientSecret(m.index, strings.TrimSpace(value))
	}
}
