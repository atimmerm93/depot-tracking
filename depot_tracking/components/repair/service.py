import hashlib
from datetime import date, timedelta
from typing import Any, Callable

from di_unit_of_work.transactional_decorator import transactional

from depot_tracking.components.data_operations.models import (
    TransactionUpdateModel,
    TransactionWriteModel,
)
from depot_tracking.components.data_operations.product_repository import ProductRepository
from depot_tracking.components.data_operations.transaction_data_operations import TransactionDataOperations
from depot_tracking.components.data_operations.transaction_repository import TransactionRepository
from depot_tracking.components.shared import IdentifierCanonicalizer, RepairRulesLoader
from depot_tracking.core.models import TransactionType


class RepairService:
    def __init__(
            self,
            *,
            product_repo: ProductRepository,
            tx_repo: TransactionRepository,
            tx_ops: TransactionDataOperations,
            identifier_canonicalizer: IdentifierCanonicalizer,
            repair_rules_loader: RepairRulesLoader,
    ) -> None:
        self._product_repo = product_repo
        self._tx_repo = tx_repo
        self._tx_ops = tx_ops
        self._identifier_canonicalizer = identifier_canonicalizer
        self._repair_rules_loader = repair_rules_loader

    @transactional
    def repair_known_data_issues(
            self,
            *,
            load_repair_rules_config: Callable[[], dict[str, Any]] | None = None,
    ) -> dict[str, int]:
        stats = {"applied": 0, "skipped": 0, "errors": 0}
        repair_rules = (load_repair_rules_config or self._load_repair_rules_config)()

        try:
            stats["applied"] += self._migrate_legacy_split_repairs_to_split_type(self._tx_repo, self._tx_ops)
            for alias_wkn, canonical_wkn in self._identifier_canonicalizer.iter_legacy_consors_aliases():
                stats["applied"] += self._purge_legacy_consors_alias_inferred_transactions(
                    self._product_repo,
                    self._tx_repo,
                    self._tx_ops,
                    alias_wkn=alias_wkn,
                )
            for item in repair_rules.get("alias_neutralizations", []):
                stats["applied"] += self._neutralize_alias_inferred_buys(
                    self._product_repo,
                    self._tx_repo,
                    self._tx_ops,
                    alias_wkn=str(item["alias_wkn"]).upper(),
                    canonical_wkn=str(item["canonical_wkn"]).upper(),
                )
            for item in repair_rules.get("split_adjustments", []):
                stats["applied"] += self._apply_split_adjustment(
                    self._product_repo,
                    self._tx_repo,
                    self._tx_ops,
                    wkn=str(item["wkn"]).upper(),
                    split_date=date.fromisoformat(str(item["split_date"])),
                    ratio=float(item["ratio"]),
                    label=str(item["label"]),
                )
            for item in repair_rules.get("share_exchange_adjustments", []):
                stats["applied"] += self._apply_share_exchange_adjustment(
                    self._product_repo,
                    self._tx_repo,
                    self._tx_ops,
                    source_wkn=str(item["source_wkn"]).upper(),
                    target_wkn=str(item["target_wkn"]).upper(),
                    exchange_date=date.fromisoformat(str(item["exchange_date"])),
                    ratio=float(item["ratio"]),
                    label=str(item["label"]),
                )
            for item in repair_rules.get("manual_missing_buys", []):
                stats["applied"] += self._ensure_manual_missing_buy(
                    self._product_repo,
                    self._tx_repo,
                    self._tx_ops,
                    wkn=str(item["wkn"]).upper(),
                    buy_date=date.fromisoformat(str(item["buy_date"])),
                    quantity=float(item["quantity"]),
                    gross_amount=float(item["gross_amount"]),
                    costs=float(item.get("costs", 0.0)),
                    label=str(item["label"]),
                    bank=str(item.get("bank", "UNKNOWN")).upper(),
                )
            for item in repair_rules.get("manual_buys", []):
                stats["applied"] += self._ensure_manual_buy(
                    self._product_repo,
                    self._tx_repo,
                    self._tx_ops,
                    wkn=str(item["wkn"]).upper(),
                    buy_date=date.fromisoformat(str(item["buy_date"])),
                    quantity=float(item["quantity"]),
                    gross_amount=float(item["gross_amount"]),
                    costs=float(item.get("costs", 0.0)),
                    label=str(item["label"]),
                    bank=str(item.get("bank", "UNKNOWN")).upper(),
                    source_file=str(item.get("source_file", "")).strip() or None,
                )
        except Exception:
            stats["errors"] += 1
            raise

        return stats

    def _load_repair_rules_config(self) -> dict[str, Any]:
        return self._repair_rules_loader.load()

    @staticmethod
    def _migrate_legacy_split_repairs_to_split_type(
            tx_repo: TransactionRepository,
            tx_ops: TransactionDataOperations,
    ) -> int:
        rows = tx_repo.list_legacy_split_repairs()

        applied = 0
        for row in rows:
            qty = float(row.quantity or 0.0)
            tx_ops.update(
                TransactionUpdateModel(
                    transaction_id=row.id,
                    type=TransactionType.SPLIT,
                    quantity=qty if row.type == TransactionType.BUY else -qty,
                    gross_amount=float(row.gross_amount or 0.0),
                    costs=0.0,
                )
            )
            applied += 1

        if applied:
            print(f"[REPAIR][OK] Migrated {applied} legacy split repair rows to SPLIT type")
        return applied

    @staticmethod
    def _neutralize_alias_inferred_buys(
            product_repo: ProductRepository,
            tx_repo: TransactionRepository,
            tx_ops: TransactionDataOperations,
            *,
            alias_wkn: str,
            canonical_wkn: str,
    ) -> int:
        product = product_repo.get_by_wkn(alias_wkn)
        if product is None:
            return 0

        inferred_buys = tx_repo.list_by_product_and_type_with_source_prefix(
            product_id=product.id,
            tx_type=TransactionType.BUY,
            prefix="inferred_from_depotauszug:",
        )

        applied = 0
        for tx in inferred_buys:
            close_hash = hashlib.sha256(
                f"repair-alias-close|{alias_wkn}|{canonical_wkn}|{tx.id}".encode("utf-8")
            ).hexdigest()
            if tx_repo.exists_by_source_hash(close_hash):
                continue

            close_gross = float(tx.gross_amount) + float(tx.costs)
            tx_ops.create(
                TransactionWriteModel(
                    product_id=tx.product_id,
                    type=TransactionType.SELL,
                    transaction_date=tx.transaction_date,
                    quantity=tx.quantity,
                    gross_amount=close_gross,
                    costs=0.0,
                    currency="EUR",
                    bank=tx.bank,
                    source_file=f"repair_alias_close:{alias_wkn}->{canonical_wkn}",
                    source_hash=close_hash,
                )
            )
            applied += 1
            print(
                f"[REPAIR][OK] Closed inferred alias BUY for {alias_wkn} tx_id={tx.id} qty={tx.quantity} at zero P/L"
            )

        return applied

    @staticmethod
    def _purge_legacy_consors_alias_inferred_transactions(
            product_repo: ProductRepository,
            tx_repo: TransactionRepository,
            tx_ops: TransactionDataOperations,
            *,
            alias_wkn: str,
    ) -> int:
        product = product_repo.get_by_wkn(alias_wkn)
        if product is None:
            return 0

        rows = tx_repo.list_by_product_with_source_prefixes(
            product_id=product.id,
            prefixes=("inferred_from_depotauszug:", "repair_alias_close:"),
        )

        removed = 0
        for row in rows:
            tx_ops.delete_by_id(row.id)
            removed += 1
        if removed:
            print(f"[REPAIR][OK] Purged {removed} legacy inferred rows for alias WKN {alias_wkn}")
        return removed

    @staticmethod
    def _apply_split_adjustment(
            product_repo: ProductRepository,
            tx_repo: TransactionRepository,
            tx_ops: TransactionDataOperations,
            *,
            wkn: str,
            split_date: date,
            ratio: float,
            label: str,
    ) -> int:
        if ratio <= 1:
            return 0

        product = product_repo.get_by_wkn(wkn)
        if product is None:
            return 0

        qty_before = tx_repo.sum_signed_quantity_before(product_id=product.id, before_date=split_date)
        if qty_before <= 0:
            return 0

        desired_adjust_qty = qty_before * (ratio - 1.0)
        if desired_adjust_qty <= 0:
            return 0

        existing_rows = tx_repo.list_split_transactions_by_prefix(
            product_id=product.id,
            tx_date=split_date,
            source_prefix=f"repair_split:{wkn}:",
        )
        existing_adjust_qty = sum(item.quantity for item in existing_rows if split_date.isoformat() in item.source_file)

        delta_qty = round(desired_adjust_qty - existing_adjust_qty, 8)
        if abs(delta_qty) < 1e-8:
            return 0

        bank = tx_repo.get_latest_non_unknown_bank_for_product(product.id)
        tx_ops.create(
            TransactionWriteModel(
                product_id=product.id,
                type=TransactionType.SPLIT,
                transaction_date=split_date,
                quantity=delta_qty,
                gross_amount=0.0,
                costs=0.0,
                currency="EUR",
                bank=bank,
                source_file=f"repair_split:{wkn}:{ratio:g}:{split_date.isoformat()}:{label}",
                source_hash=hashlib.sha256(
                    f"repair_service-split-adjust|{wkn}|{split_date.isoformat()}|{ratio:.8f}|"
                    f"{desired_adjust_qty:.8f}|{existing_adjust_qty:.8f}|{delta_qty:.8f}|{label}".encode("utf-8")
                ).hexdigest(),
            )
        )
        print(
            f"[REPAIR][OK] Applied split adjustment for {wkn}: pre={qty_before} ratio={ratio:g} "
            f"desired_add={desired_adjust_qty} existing_add={existing_adjust_qty} delta={delta_qty}"
        )
        return 1

    @staticmethod
    def _apply_share_exchange_adjustment(
            product_repo: ProductRepository,
            tx_repo: TransactionRepository,
            tx_ops: TransactionDataOperations,
            *,
            source_wkn: str,
            target_wkn: str,
            exchange_date: date,
            ratio: float,
            label: str,
    ) -> int:
        if ratio <= 0:
            return 0

        source_product = product_repo.get_by_wkn(source_wkn)
        target_product = product_repo.get_by_wkn(target_wkn)
        if source_product is None or target_product is None:
            return 0

        source_qty_before = tx_repo.sum_signed_quantity_before(product_id=source_product.id, before_date=exchange_date)
        if source_qty_before <= 0:
            return 0

        source_basis_before, _ = tx_repo.get_buy_basis(
            product_id=source_product.id,
            until_date=exchange_date - timedelta(days=1),
        )

        desired_source_qty = round(-source_qty_before, 8)
        desired_target_qty = round(source_qty_before * ratio, 8)
        desired_source_basis = round(-source_basis_before, 2)
        desired_target_basis = round(source_basis_before, 2)

        source_prefix = (
            f"repair_exchange:{source_wkn}->{target_wkn}:{ratio:g}:{exchange_date.isoformat()}:{label}:source"
        )
        target_prefix = (
            f"repair_exchange:{source_wkn}->{target_wkn}:{ratio:g}:{exchange_date.isoformat()}:{label}:target"
        )

        existing_source_rows = tx_repo.list_split_transactions_by_prefix(
            product_id=source_product.id,
            tx_date=exchange_date,
            source_prefix=source_prefix,
        )
        existing_target_rows = tx_repo.list_split_transactions_by_prefix(
            product_id=target_product.id,
            tx_date=exchange_date,
            source_prefix=target_prefix,
        )
        existing_source_qty = sum(item.quantity for item in existing_source_rows)
        existing_target_qty = sum(item.quantity for item in existing_target_rows)
        existing_source_basis = sum(item.gross_amount for item in existing_source_rows)
        existing_target_basis = sum(item.gross_amount for item in existing_target_rows)

        delta_source_qty = round(desired_source_qty - existing_source_qty, 8)
        delta_target_qty = round(desired_target_qty - existing_target_qty, 8)
        delta_source_basis = round(desired_source_basis - existing_source_basis, 2)
        delta_target_basis = round(desired_target_basis - existing_target_basis, 2)

        applied = 0
        if abs(delta_source_qty) >= 1e-8 or abs(delta_source_basis) >= 0.005:
            tx_ops.create(
                TransactionWriteModel(
                    product_id=source_product.id,
                    type=TransactionType.SPLIT,
                    transaction_date=exchange_date,
                    quantity=delta_source_qty,
                    gross_amount=delta_source_basis,
                    costs=0.0,
                    currency="EUR",
                    bank=tx_repo.get_latest_non_unknown_bank_for_product(source_product.id),
                    source_file=f"{source_prefix}:delta",
                    source_hash=hashlib.sha256(
                        (
                            f"repair_service-exchange-source|{source_wkn}|{target_wkn}|{exchange_date.isoformat()}|{ratio:.8f}|"
                            f"{desired_source_qty:.8f}|{existing_source_qty:.8f}|{delta_source_qty:.8f}|"
                            f"{desired_source_basis:.2f}|{existing_source_basis:.2f}|{delta_source_basis:.2f}"
                        ).encode("utf-8")
                    ).hexdigest(),
                )
            )
            applied += 1

        if abs(delta_target_qty) >= 1e-8 or abs(delta_target_basis) >= 0.005:
            tx_ops.create(
                TransactionWriteModel(
                    product_id=target_product.id,
                    type=TransactionType.SPLIT,
                    transaction_date=exchange_date,
                    quantity=delta_target_qty,
                    gross_amount=delta_target_basis,
                    costs=0.0,
                    currency="EUR",
                    bank=tx_repo.get_latest_non_unknown_bank_for_product(target_product.id),
                    source_file=f"{target_prefix}:delta",
                    source_hash=hashlib.sha256(
                        (
                            f"repair_service-exchange-target|{source_wkn}|{target_wkn}|{exchange_date.isoformat()}|{ratio:.8f}|"
                            f"{desired_target_qty:.8f}|{existing_target_qty:.8f}|{delta_target_qty:.8f}|"
                            f"{desired_target_basis:.2f}|{existing_target_basis:.2f}|{delta_target_basis:.2f}"
                        ).encode("utf-8")
                    ).hexdigest(),
                )
            )
            applied += 1

        if applied:
            print(
                f"[REPAIR][OK] Applied share exchange {source_wkn}->{target_wkn} on {exchange_date}: "
                f"source_qty={source_qty_before}, ratio={ratio:g}"
            )
        return applied

    @staticmethod
    def _ensure_manual_missing_buy(
            product_repo: ProductRepository,
            tx_repo: TransactionRepository,
            tx_ops: TransactionDataOperations,
            *,
            wkn: str,
            buy_date: date,
            quantity: float,
            gross_amount: float,
            costs: float,
            label: str,
            bank: str,
    ) -> int:
        if quantity <= 0 or gross_amount < 0 or costs < 0:
            return 0

        product = product_repo.get_by_wkn(wkn)
        if product is None:
            return 0

        existing_buys = tx_repo.list_by_product_and_type(product_id=product.id, tx_type=TransactionType.BUY)
        if existing_buys:
            return 0

        return RepairService._insert_manual_buy(
            tx_repo,
            tx_ops,
            product_id=product.id,
            wkn=wkn,
            buy_date=buy_date,
            quantity=quantity,
            gross_amount=gross_amount,
            costs=costs,
            label=label,
            bank=bank,
            source_file=f"repair_missing_buy:{wkn}:{label}",
            key_prefix="repair_service-missing-buy",
        )

    @staticmethod
    def _ensure_manual_buy(
            product_repo: ProductRepository,
            tx_repo: TransactionRepository,
            tx_ops: TransactionDataOperations,
            *,
            wkn: str,
            buy_date: date,
            quantity: float,
            gross_amount: float,
            costs: float,
            label: str,
            bank: str,
            source_file: str | None = None,
    ) -> int:
        if quantity <= 0 or gross_amount < 0 or costs < 0:
            return 0

        product = product_repo.get_by_wkn(wkn)
        if product is None:
            return 0

        resolved_source = source_file or f"repair_manual_buy:{wkn}:{label}"
        return RepairService._insert_manual_buy(
            tx_repo,
            tx_ops,
            product_id=product.id,
            wkn=wkn,
            buy_date=buy_date,
            quantity=quantity,
            gross_amount=gross_amount,
            costs=costs,
            label=label,
            bank=bank,
            source_file=resolved_source,
            key_prefix="manual-buy",
        )

    @staticmethod
    def _insert_manual_buy(
            tx_repo: TransactionRepository,
            tx_ops: TransactionDataOperations,
            *,
            product_id: int,
            wkn: str,
            buy_date: date,
            quantity: float,
            gross_amount: float,
            costs: float,
            label: str,
            bank: str,
            source_file: str,
            key_prefix: str,
    ) -> int:
        repair_key = (
            f"{key_prefix}|{wkn}|{buy_date.isoformat()}|{quantity:.8f}|{gross_amount:.2f}|{costs:.2f}|"
            f"{bank}|{source_file}|{label}"
        )
        repair_hash = hashlib.sha256(repair_key.encode("utf-8")).hexdigest()
        if tx_repo.exists_by_source_hash(repair_hash):
            return 0

        if tx_repo.exists_exact_buy(
                product_id=product_id,
                transaction_date=buy_date,
                quantity=float(quantity),
                gross_amount=float(gross_amount),
                costs=float(costs),
                bank=bank,
                source_file=source_file,
        ):
            return 0

        tx_ops.create(
            TransactionWriteModel(
                product_id=product_id,
                type=TransactionType.BUY,
                transaction_date=buy_date,
                quantity=float(quantity),
                gross_amount=float(gross_amount),
                costs=float(costs),
                currency="EUR",
                bank=bank,
                source_file=source_file,
                source_hash=repair_hash,
            )
        )
        print(f"[REPAIR][OK] Added manual BUY repair_service for {wkn}: qty={quantity} gross={gross_amount:.2f} EUR")
        return 1
