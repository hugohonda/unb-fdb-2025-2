-- Triggers com comandos condicionais para validações e auditoria
-- PostgreSQL

-- Trigger: Validação antes de inserir preço PF
CREATE OR REPLACE FUNCTION trg_validar_preco_pf()
RETURNS TRIGGER AS $$
BEGIN
    -- Validação: Preço deve ser positivo
    IF NEW.pf_com_impostos IS NOT NULL AND NEW.pf_com_impostos <= 0 THEN
        RAISE EXCEPTION 'Preço Fábrica deve ser maior que zero';
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_validar_preco_pf_insert
    BEFORE INSERT ON precos_fabrica
    FOR EACH ROW
    EXECUTE FUNCTION trg_validar_preco_pf();

CREATE TRIGGER trg_validar_preco_pf_update
    BEFORE UPDATE ON precos_fabrica
    FOR EACH ROW
    EXECUTE FUNCTION trg_validar_preco_pf();

-- Trigger: Auditoria ao inserir/atualizar preço PF
CREATE OR REPLACE FUNCTION trg_auditoria_preco_pf()
RETURNS TRIGGER AS $$
BEGIN
    IF TG_OP = 'INSERT' THEN
        INSERT INTO historico_precos 
            (id_produto, tipo_preco, id_aliquota, valor_anterior, valor_novo, usuario_alteracao)
        VALUES 
            (NEW.id_produto, 'PF', NEW.id_aliquota, NULL, NEW.pf_com_impostos, 'SISTEMA_TRIGGER');
    ELSIF TG_OP = 'UPDATE' AND OLD.pf_com_impostos IS DISTINCT FROM NEW.pf_com_impostos THEN
        INSERT INTO historico_precos 
            (id_produto, tipo_preco, id_aliquota, valor_anterior, valor_novo, usuario_alteracao)
        VALUES 
            (NEW.id_produto, 'PF', NEW.id_aliquota, OLD.pf_com_impostos, NEW.pf_com_impostos, 'SISTEMA_TRIGGER');
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_auditoria_preco_pf_insert
    AFTER INSERT ON precos_fabrica
    FOR EACH ROW
    EXECUTE FUNCTION trg_auditoria_preco_pf();

CREATE TRIGGER trg_auditoria_preco_pf_update
    AFTER UPDATE ON precos_fabrica
    FOR EACH ROW
    EXECUTE FUNCTION trg_auditoria_preco_pf();

-- Trigger: Validação e auditoria PMVG
CREATE OR REPLACE FUNCTION trg_validar_pmvg_vs_pf()
RETURNS TRIGGER AS $$
DECLARE
    v_pf_valor DECIMAL(10,2);
    v_cap tipo_sim_nao;
BEGIN
    -- Validação: Preço deve ser positivo
    IF NEW.pmvg_com_impostos IS NOT NULL AND NEW.pmvg_com_impostos <= 0 THEN
        RAISE EXCEPTION 'PMVG deve ser maior que zero';
    END IF;
    
    -- Obtém valor do PF e se tem CAP
    SELECT pf.pf_com_impostos, p.cap INTO v_pf_valor, v_cap
    FROM precos_fabrica pf
    INNER JOIN produtos p ON pf.id_produto = p.id_produto
    WHERE pf.id_produto = NEW.id_produto AND pf.id_aliquota = NEW.id_aliquota
    ORDER BY pf.data_vigencia DESC
    LIMIT 1;
    
    -- Validação condicional baseada no CAP
    IF v_pf_valor IS NOT NULL THEN
        IF v_cap = 'Sim' THEN
            -- Com CAP, permite desconto até 21.53% (com tolerância)
            IF NEW.pmvg_com_impostos > (v_pf_valor * 0.895) THEN
                -- Ajusta para valor máximo permitido
                NEW.pmvg_com_impostos := v_pf_valor * 0.7847;
            END IF;
        ELSE
            -- Sem CAP, PMVG não deve exceder PF
            IF NEW.pmvg_com_impostos > v_pf_valor THEN
                RAISE EXCEPTION 'PMVG (%) não pode ser maior que PF (%) para produtos sem CAP', 
                    NEW.pmvg_com_impostos, v_pf_valor;
            END IF;
        END IF;
    END IF;
    
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_validar_pmvg_vs_pf_insert
    BEFORE INSERT ON precos_pmvg
    FOR EACH ROW
    EXECUTE FUNCTION trg_validar_pmvg_vs_pf();

CREATE TRIGGER trg_validar_pmvg_vs_pf_update
    BEFORE UPDATE ON precos_pmvg
    FOR EACH ROW
    EXECUTE FUNCTION trg_validar_pmvg_vs_pf();

-- Trigger: Auditoria PMVG
CREATE OR REPLACE FUNCTION trg_auditoria_preco_pmvg()
RETURNS TRIGGER AS $$
BEGIN
    IF TG_OP = 'INSERT' THEN
        INSERT INTO historico_precos 
            (id_produto, tipo_preco, id_aliquota, valor_anterior, valor_novo, usuario_alteracao)
        VALUES 
            (NEW.id_produto, 'PMVG', NEW.id_aliquota, NULL, NEW.pmvg_com_impostos, 'SISTEMA_TRIGGER');
    ELSIF TG_OP = 'UPDATE' AND OLD.pmvg_com_impostos IS DISTINCT FROM NEW.pmvg_com_impostos THEN
        INSERT INTO historico_precos 
            (id_produto, tipo_preco, id_aliquota, valor_anterior, valor_novo, usuario_alteracao)
        VALUES 
            (NEW.id_produto, 'PMVG', NEW.id_aliquota, OLD.pmvg_com_impostos, NEW.pmvg_com_impostos, 'SISTEMA_TRIGGER');
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_auditoria_preco_pmvg_insert
    AFTER INSERT ON precos_pmvg
    FOR EACH ROW
    EXECUTE FUNCTION trg_auditoria_preco_pmvg();

CREATE TRIGGER trg_auditoria_preco_pmvg_update
    AFTER UPDATE ON precos_pmvg
    FOR EACH ROW
    EXECUTE FUNCTION trg_auditoria_preco_pmvg();

-- Trigger: Auditoria automática de alterações em produtos
CREATE OR REPLACE FUNCTION trg_auditoria_produto()
RETURNS TRIGGER AS $$
BEGIN
    -- Registra mudanças importantes nos campos do produto
    IF OLD.cap IS DISTINCT FROM NEW.cap THEN
        INSERT INTO historico_precos 
            (id_produto, tipo_preco, id_aliquota, valor_anterior, valor_novo, usuario_alteracao)
        VALUES 
            (NEW.id_produto, 'PF', NULL, 
             'CAP: ' || OLD.cap::TEXT, 
             'CAP: ' || NEW.cap::TEXT, 
             'SISTEMA_TRIGGER');
    END IF;
    
    IF OLD.id_regime IS DISTINCT FROM NEW.id_regime THEN
        INSERT INTO historico_precos 
            (id_produto, tipo_preco, id_aliquota, valor_anterior, valor_novo, usuario_alteracao)
        VALUES 
            (NEW.id_produto, 'PF', NULL, 
             'Regime: ' || (SELECT regime_preco FROM regimes_preco WHERE id_regime = OLD.id_regime), 
             'Regime: ' || (SELECT regime_preco FROM regimes_preco WHERE id_regime = NEW.id_regime), 
             'SISTEMA_TRIGGER');
    END IF;
    
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_auditoria_produto_update
    AFTER UPDATE ON produtos
    FOR EACH ROW
    EXECUTE FUNCTION trg_auditoria_produto();

-- Trigger: Atualização automática de data_atualizacao
CREATE OR REPLACE FUNCTION trg_atualizar_data_produto()
RETURNS TRIGGER AS $$
BEGIN
    NEW.data_atualizacao := CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_atualizar_data_produto
    BEFORE UPDATE ON produtos
    FOR EACH ROW
    EXECUTE FUNCTION trg_atualizar_data_produto();
