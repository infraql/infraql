package symtab

import (
	"fmt"

	"vitess.io/vitess/go/vt/sqlparser"

	log "github.com/sirupsen/logrus"
)

type SymTabEntry struct {
	Type string
	Data interface{}
}

func NewSymTabEntry(t string, data interface{}) SymTabEntry {
	return SymTabEntry{
		Type: t,
		Data: data,
	}
}

type SymTab interface {
	GetSymbol(interface{}) (SymTabEntry, error)
	SetSymbol(interface{}, SymTabEntry) error
}

type HashMapTreeSymTab struct {
	tab    map[interface{}]SymTabEntry
	leaves map[interface{}]SymTab
}

func NewHashMapTreeSymTab() HashMapTreeSymTab {
	return HashMapTreeSymTab{
		tab:    make(map[interface{}]SymTabEntry),
		leaves: make(map[interface{}]SymTab),
	}
}

func (st HashMapTreeSymTab) GetSymbol(k interface{}) (SymTabEntry, error) {
	switch k := k.(type) {
	case *sqlparser.ColName:
		log.Infoln(fmt.Sprintf("reading from symbol table using ColIdent %v", k))
		return st.GetSymbol(k.Name.GetRawVal())
	}
	v, ok := st.tab[k]
	if ok {
		return v, nil
	}
	for _, v := range st.leaves {
		lv, err := v.GetSymbol(k)
		if err == nil {
			return lv, nil
		}
	}
	return SymTabEntry{}, fmt.Errorf("could not locate symbol %v", k)
}

func (st HashMapTreeSymTab) SetSymbol(k interface{}, v SymTabEntry) error {
	_, ok := st.tab[k]
	if ok {
		return fmt.Errorf("symbol %v already present in symtab", k)
	}
	st.tab[k] = v
	return nil
}

func (st HashMapTreeSymTab) SetLeaf(k interface{}, v SymTab) error {
	_, ok := st.leaves[k]
	if ok {
		return fmt.Errorf("leaf symtab %v already present in symtab", k)
	}
	st.leaves[k] = v
	return nil
}
