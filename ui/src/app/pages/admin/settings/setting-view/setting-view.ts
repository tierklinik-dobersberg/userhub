import { ChangeDetectionStrategy, ChangeDetectorRef, Component, OnDestroy, OnInit, ViewChild } from "@angular/core";
import { NgModel } from "@angular/forms";
import { DomSanitizer } from "@angular/platform-browser";
import { ActivatedRoute, Router } from "@angular/router";
import { NzMessageService } from "ng-zorro-antd/message";
import { forkJoin, Observable, Subject, throwError } from "rxjs";
import { map, switchMap, takeUntil } from "rxjs/operators";
import { ConfigAPI, Schema, SchemaInstance } from "src/app/api";
import { HeaderTitleService } from "src/app/shared/header-title";
import { extractErrorMessage } from "src/app/utils";

@Component({
  templateUrl: './setting-view.html',
  changeDetection: ChangeDetectionStrategy.OnPush,
})
export class SettingViewComponent implements OnInit, OnDestroy {
  private destroy$ = new Subject();

  @ViewChild('singleValueModel', {static: false, read: NgModel})
  singleValueModel: NgModel | null = null;

  schema: Schema | null = null;
  configs: {
    [key: string]: SchemaInstance;
  } | SchemaInstance = {}

  originalValue: {
    [key: string]: SchemaInstance
  } | SchemaInstance = {};

  singleModeID = '';

  constructor(
    private configAPI: ConfigAPI,
    private headerTitleService: HeaderTitleService,
    private route: ActivatedRoute,
    private router: Router,
    private nzMessageService: NzMessageService,
    private cdr: ChangeDetectorRef,
    public domSanitizer: DomSanitizer,
  ) {}

  saveSetting() {
    if (!this.schema) {
      return;
    }

    let stream: Observable<{warning?: string}>;

    if (!this.schema.multi) {
      if (!!this.singleModeID) {
        stream = this.configAPI.updateSetting(this.schema.name, this.singleModeID, this.configs)
      } else {
        stream = this.configAPI.createSetting(this.schema.name, this.configs)
      }
    } else {
      stream = throwError("not yet supported");
    }

    stream.subscribe({
            next: res => {
              if (!!res.warning) {
                this.nzMessageService.warning(res.warning)
              } else {
                this.nzMessageService.success("Einstellungen erfolgreich gespeichert")
              }
            },
            error: err => this.nzMessageService.error(extractErrorMessage(err, "Fehler"))
          })
  }

  ngOnInit(): void {
      this.route.paramMap
        .pipe(
          map(params => (params.get("name") || '').toLowerCase()),
          switchMap(name => {
            return forkJoin({
              schema: this.configAPI.listSchemas().pipe(map(schemas => schemas.find(s => s.name.toLowerCase() === name))),
              settings: this.configAPI.getSettings(name),
            })
          }),
          takeUntil(this.destroy$),
        )
        .subscribe(result => {
          if (!result.schema) {
            this.router.navigate(["/admin/settings"]);
            return;
          }

          this.headerTitleService.set(
            result.schema.displayName || result.schema.name,
            '',
            null,
            [
              {name: 'Administration', route: '/admin/'},
            ]
          )

          this.schema = result.schema;
          this.configs = result.settings;

          // If this kind of configuration can only exist once make sure
          // we have an empty model to work with.
          if (!this.schema.multi) {
            this.singleModeID = Object.keys(result.settings)[0] || '';
            if (!!this.singleModeID) {
              this.configs = result.settings[this.singleModeID]
            } else {
              this.configs = {};
            }

            this.originalValue = {...this.configs};
          } else {
            this.originalValue = {}
            Object.keys(result.settings).forEach(key => {
              this.originalValue[key] = {
                ...result.settings[key],
              }
            })
          }
          this.cdr.markForCheck();
        })
  }

  ngOnDestroy(): void {
      this.destroy$.next();
      this.destroy$.complete();
  }
}
