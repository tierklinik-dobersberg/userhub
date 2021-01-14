import { HttpClient } from "@angular/common/http";
import { Injectable } from "@angular/core";
import { BehaviorSubject, Observable, of } from "rxjs";
import { catchError } from "rxjs/operators";
import { IdentityAPI } from "./identity.api";

export interface ExternalLink {
  ParentMenu: string;
  Text: string;
  Icon: string;
  RequiresRole: string[];
  Link: string;
  BlankTarget: boolean;
}

export interface UIConfig {
  HideUsersWithRole?: string[];
  ExternalLinks?: ExternalLink[];
}

@Injectable({
  providedIn: 'root'
})
export class ConfigAPI {
  private onChange = new BehaviorSubject<UIConfig | null>(null);

  get change(): Observable<UIConfig | null> {
    return this.onChange;
  }

  get current(): UIConfig | null {
    return this.onChange.getValue()
  }

  constructor(
    private http: HttpClient,
    private identity: IdentityAPI,
  ) {
    this.identity.profileChange.subscribe(() => {
      this.loaddUIConfig()
        .pipe(catchError(err => {
          return of(null);
        }))
        .subscribe(cfg => {
          this.onChange.next(cfg);
        })
    });
  }

  loaddUIConfig(): Observable<UIConfig> {
    return this.http.get<UIConfig>('/api/config/v1/ui');
  }
}
