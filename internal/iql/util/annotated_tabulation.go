package util

import (
	"infraql/internal/iql/dto"
	"infraql/internal/iql/metadata"
)

type AnnotatedTabulation struct {
	tab  *metadata.Tabulation
	hIds *dto.HeirarchyIdentifiers
}

func NewAnnotatedTabulation(tab *metadata.Tabulation, hIds *dto.HeirarchyIdentifiers) AnnotatedTabulation {
	return AnnotatedTabulation{
		tab:  tab,
		hIds: hIds,
	}
}

func (at AnnotatedTabulation) GetTabulation() *metadata.Tabulation {
	return at.tab
}

func (at AnnotatedTabulation) GetHeirarchyIdentifiers() *dto.HeirarchyIdentifiers {
	return at.hIds
}
